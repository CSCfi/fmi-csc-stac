import json
import getpass
from pathlib import Path
import requests
import pystac_client
from requests.auth import HTTPBasicAuth
from urllib.parse import urljoin

def json_convert(jsonfile):

    """
        jsonfile: json file in dict format
        
        A function to map the Sentinel-2 STAC jsonfiles into the GeoServer database layout.
        There are different json layouts for Collections and Items. The function checks if the jsonfile is of type "Collection",
        or of type "Feature" (=Item). A number of properties are hardcoded into Sentinel-2 metadata as these are not collected in the STAC jsonfiles.
    """

    with open(jsonfile) as f:
        content = json.load(f)
    
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
                "derivedFrom": {
                    "href": content["derived_from"],
                    "rel": "derived_from",
                    "type": "application/geo+json"
                }, # Derived_from added
                "summaries": content["summaries"],
                "queryables": [
                    "eo:identifier"
                ]
            }
        }

        if "assets" in content:
            new_json["properties"]["assets"] = content["assets"]

        for link in content["links"]:
            if link["rel"] == "license":
                new_json["properties"]["licenseURL"] = link["href"]

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

    return json.loads(json.dumps(new_json))

if __name__ == "__main__":

    pwd = getpass.getpass()

    collection_name = "sentinel_2_monthly_index_mosaics_at_fmi"

    workingdir = Path(__file__).parent
    sentinel = workingdir / "FMI" / collection_name

    app_host = "http://86.50.229.158:8080/geoserver/rest/oseo/"
    catalog = pystac_client.Client.open("http://86.50.229.158:8080/geoserver/ogc/stac/")

    # Convert the STAC collection json into json that GeoServer can handle
    converted = json_convert(sentinel / "collection.json")

    #Additional code for changing collection data if the collection already exists
    collections = catalog.get_collections()
    col_ids = [col.id for col in collections]
    if collection_name in col_ids:
        r = requests.put(urljoin(app_host + "collections/", collection_name), json=converted, auth=HTTPBasicAuth("admin", pwd))
        r.raise_for_status()
        print(f"Updated {collection_name}")
    else:
        r = requests.post(urljoin(app_host, "collections/"), json=converted, auth=HTTPBasicAuth("admin", pwd))
        r.raise_for_status()
        print(f"Added new collection")

    # Get the posted items from the specific collection
    posted = catalog.search(collections=[collection_name]).item_collection()
    posted_ids = [x.id for x in posted]
    print(f"POSTed: {len(posted_ids)}")

    with open(sentinel / "collection.json") as f:
        rootcollection = json.load(f)

    items = [x['href'] for x in rootcollection["links"] if x["rel"] == "item"]

    print("POSTing items:")
    for i, item in enumerate(items):
        with open(sentinel / item) as f:
            payload = json.load(f)
        # Convert the STAC item json into json that GeoServer can handle
        converted = json_convert(sentinel / item)
        request_point = f"collections/{rootcollection['id']}/products"
        if payload["id"] in posted_ids:
            request_point = f"collections/{rootcollection['id']}/products/{payload['id']}"
            r = requests.put(urljoin(app_host, request_point), json=converted, auth=HTTPBasicAuth("admin", pwd))
            r.raise_for_status()
        else:
            r = requests.post(urljoin(app_host, request_point), json=converted, auth=HTTPBasicAuth("admin", pwd))
            r.raise_for_status()
        print("*", end='', flush=True) # Just to keep track that the script is still running
    print("")