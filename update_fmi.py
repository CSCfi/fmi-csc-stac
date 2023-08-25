import json
import argparse
import getpass
import requests
import pystac_client
import pandas as pd
import rasterio
import urllib.request
from requests.auth import HTTPBasicAuth
from urllib.parse import urljoin
from pystac import Collection, Item

def change_to_https(request: requests.Request) -> requests.Request: 
    request.url = request.url.replace("http:", "https:")
    return request

def retry_errors(list_of_items, list_of_errors):
    """
    Function to retry retrieving the items that were timed out during the process.
    
    list_of_items - List containing the STAC items from the source. The errored items will be appended to this list when successfully retrieved
    list_of_errors - List of links to the items that timed out during the retrieving process. Function will run until this list is empty
    """

    print(" * Trying to add items that timedout")
    while len(list_of_errors) > 0:
        for i,item in enumerate(list_of_errors):
            try:
                list_of_items.append(Item.from_file(item))
                print(f" * Added item {item}")
                list_of_errors.remove(item)
            except Exception as e:
                print(f" ! Timeout on {item} #{i}")

def json_convert(content):

    """ 
    A function to map the Sentinel-2 STAC dictionaries into the GeoServer database layout.
    There are different json layouts for Collections and Items. The function checks if the dictionary is of type "Collection",
    or of type "Feature" (=Item).

    content - STAC dictionary from where the modified JSON will be made
    """
    
    if content["type"] == "Collection":

        new_json = {
            "type": "Feature",
            "geometry": {
                "type": "Polygon",
                "coordinates": [
                    [
                        [
                            content["extent"]["spatial"]["bbox"][0][2],
                            content["extent"]["spatial"]["bbox"][0][1]
                        ],
                        [
                            content["extent"]["spatial"]["bbox"][0][2],
                            content["extent"]["spatial"]["bbox"][0][3]
                        ],
                        [
                            content["extent"]["spatial"]["bbox"][0][0],
                            content["extent"]["spatial"]["bbox"][0][3]
                        ],
                        [
                            content["extent"]["spatial"]["bbox"][0][0],
                            content["extent"]["spatial"]["bbox"][0][1]
                        ],
                        [
                            content["extent"]["spatial"]["bbox"][0][2],
                            content["extent"]["spatial"]["bbox"][0][1]
                        ]

                    ]
                ]
            },
            "properties": {
                "name": content["id"],
                "title": content["title"],
                "eo:identifier": content["id"],
                "description": content["description"],
                "timeStart": content["extent"]["temporal"]["interval"][0][0],
                "timeEnd": content["extent"]["temporal"]["interval"][0][1],
                "primary": True,
                "license": content["license"],
                "providers": content["providers"], # Providers added
                "derivedFrom": { # Derived_from added
                    "href": content["derived_from"],
                    "rel": "derived_from",
                    "type": "application/json"
                },
                "queryables": [
                    "eo:identifier"
                ]
            }
        }

        if "assets" in content:
            new_json["properties"]["assets"] = content["assets"]

        for link in content["links"]:
            if link["rel"] == "license":
                new_json["properties"]["licenseLink"] = { #New License URL link
                    "href": link["href"],
                    "rel": "license",
                    "type": "application/json"
                }

    if content["type"] == "Feature":

        new_json = {
            "type": "Feature",
            "geometry": content["geometry"],
            "properties": {
                "eop:identifier": content["id"],
                "eop:parentIdentifier": content["collection"],
                "timeStart": content["properties"]["start_datetime"],
                "timeEnd": content["properties"]["end_datetime"],
                "eop:resolution": content["gsd"],
                # "opt:cloudCover": int(content["properties"]["eo:cloud_cover"]),
                "crs": content["proj:epsg"],
                "projTransform": content["proj:transform"],
                # "thumbnailURL": content["links"]["thumbnail"]["href"],
                "assets": content["assets"]
            }
        }

        if content["properties"]["start_datetime"] is None and content["properties"]["end_datetime"] is None and content["properties"]["datetime"] is not None:
            new_json["properties"]["timeStart"] = content["properties"]["datetime"]
            new_json["properties"]["timeEnd"] = content["properties"]["datetime"]

    return new_json

def update_catalog(app_host, csc_catalog_client):

    """
    The main updating function of the script. Checks the collection items in the FMI catalog and compares the to the ones in CSC catalog.

    app_host - The REST API path for updating the collections
    csc_catalog_client - The STAC API path for checking which items are already in the collections
    """
    
    # Get all FMI collections from the app_host 
    csc_collections = [col for col in csc_catalog_client.get_collections() if col.id.endswith("at_fmi")]

    for collection in csc_collections:

        derived_from = [link.target for link in collection.links if link.rel == "derived_from"]
        try:
            fmi_collection = Collection.from_file(derived_from[0])
        except ValueError:
            with urllib.request.urlopen(derived_from[0]) as url:
                data = json.load(url)
                data["extent"]["temporal"]["interval"] = [data["extent"]["temporal"]["interval"]]
                fmi_collection = Collection.from_dict(data)

        fmi_collection.id = collection.id
        print(f"# Checking collection {collection.id}:")
        fmi_collection_links = fmi_collection.get_child_links()

        sub_collections = []
        for link in fmi_collection_links:
            try:
                sub_collections.append(Collection.from_file(link.target))
            except ValueError:
                with urllib.request.urlopen(link.target) as url:
                    data = json.load(url)
                    data["extent"]["temporal"]["interval"] = [data["extent"]["temporal"]["interval"]]
                    sub_collections.append(Collection.from_dict(data))

        item_links = list(set([link.target for sub in sub_collections for link in sub.get_item_links()]))
        csc_item_ids = [x.id for x in collection.get_items()]

        items = []
        errors = []
        for i,item in enumerate(item_links):
            try:
                items.append(Item.from_file(item))
            except Exception as e:
                print(f" ! Timeout on {item} #{i}")
                errors.append(item)

        # If there were connection errors during the item making process, the item generation for errors is retried
        if len(errors) > 0:
            retry_errors(items, errors)
            print(" * All errors fixed")

        print(f" * Number of items in CSC STAC and FMI for {collection.id}: {len(csc_item_ids)}/{len(items)}")

        for item in items:
            if item.id not in csc_item_ids:
                fmi_collection.add_item(item)

                with rasterio.open(next(iter(item.assets.values())).href) as src:
                    item.extra_fields["gsd"] = src.res[0]
                    item.extra_fields["proj:epsg"] = src.crs.to_string()
                    item.extra_fields["proj:transform"] = [
                        src.transform.a,
                        src.transform.b,
                        src.transform.c,
                        src.transform.d,
                        src.transform.e,
                        src.transform.f,
                        src.transform.g,
                        src.transform.h,
                        src.transform.i
                    ]

                for asset in item.assets:
                    if item.assets[asset].roles is not list:
                        item.assets[asset].roles = [item.assets[asset].roles]
    
                del item.extra_fields["license"]
                item.remove_links("license")

                item_dict = item.to_dict()
                converted = json_convert(item_dict)

                request_point = f"collections/{collection.id}/products"
                r = requests.post(urljoin(app_host, request_point), json=converted, auth=HTTPBasicAuth("admin", pwd))
                r.raise_for_status()

                print(f" + Added item {item.id} to {collection.id}")

        print(f" * All items present in {collection.id}")
    
if __name__ == "__main__":

    """
    The first check for REST API password is a --pwd argument. If there is none provided, second check is from a password file. 
    If a password file is not found, the script prompts the user to give a password through CLI
    """
    pw_filename = 'passwords.txt'
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", type=str, help="Hostname of the selected STAC API", required=True)
    parser.add_argument("--pwd", type=str, help="Password for the API")
    
    args = parser.parse_args()

    if args.pwd:
        pwd = args.pwd
    else:
        try:
            pw_file = pd.read_csv(pw_filename, header=None)
            pwd = pw_file.at[0,0]
        except FileNotFoundError:
            print("Password not given as an argument and no password file found")
            pwd = getpass.getpass()

    app_host = f"{args.host}/geoserver/rest/oseo/"
    csc_catalog_client = pystac_client.Client.open(f"{args.host}/geoserver/ogc/stac/v1/")#, request_modifier=change_to_https)

    print(f"Updating STAC Catalog at {args.host}")
    update_catalog(app_host, csc_catalog_client)