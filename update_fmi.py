import json
import getpass
from pathlib import Path
import requests
import pystac_client
from requests.auth import HTTPBasicAuth
from urllib.parse import urljoin
from pystac import Catalog, Collection
import pystac
import rasterio
import urllib.request, json


fmi_collections = [
    "https://pta.data.lit.fmi.fi/stac/catalog/Sentinel-2_global_mosaic_vuosi/Sentinel-2_global_mosaic_vuosi.json",
    "https://pta.data.lit.fmi.fi/stac/catalog/Sentinel-2_global_mosaic_dekadi/Sentinel-2_global_mosaic_dekadi.json",
    "https://pta.data.lit.fmi.fi/stac/catalog/Sentinel-2_indeksimosaiikit/Sentinel-2_indeksimosaiikit.json",
    "https://pta.data.lit.fmi.fi/stac/catalog/Sentinel-1_dekadi_mosaiikki/Sentinel-1_dekadi_mosaiikki.json",
    "https://pta.data.lit.fmi.fi/stac/catalog/Sentinel-1_daily_mosaiikki/Sentinel-1_daily_mosaiikki.json",
    "https://pta.data.lit.fmi.fi/stac/catalog/Sentinel-1_osakuvat/Sentinel-1_osakuvat.json",
    #"https://pta.data.lit.fmi.fi/stac/catalog/Landsat_pintaheijastus/Landsat_pintaheijastus.json", # THE CHILDREN ARE NOT RIGHT
    "https://pta.data.lit.fmi.fi/stac/catalog/Landsat_indeksit/Landsat_indeksit.json",
    "https://pta.data.lit.fmi.fi/stac/catalog/Metsavarateema/Metsavarateema.json",
    "https://pta.data.lit.fmi.fi/stac/catalog/Latvuskorkeusmalli/Latvuskorkeusmalli.json",
    "https://pta.data.lit.fmi.fi/stac/catalog/MML-DTM-2m/MML-DTM-2m.json",
    "https://pta.data.lit.fmi.fi/stac/catalog/Myrskytuhoriskikartta/Myrskytuhoriskikartta.json",
    "https://pta.data.lit.fmi.fi/stac/catalog/Tuulituhoriski/Tuulituhoriski.json"
]

collection_info = {
    "sentinel_2_annual_mosaics_at_fmi" : {
        "licenseURL":
            "https://ckan.ymparisto.fi/dataset/sentinel-2-satellite-image-mosaics-s2gm-sentinel-2-satelliittikuvamosaiikki-s2gm",
    },
    "sentinel_2_11_days_mosiacs_at_fmi": {
        "licenseURL":
            "https://ckan.ymparisto.fi/dataset/sentinel-2-satellite-image-mosaics-s2gm-sentinel-2-satelliittikuvamosaiikki-s2gm",
    },
    "sentinel_2_monthly_index_mosaics_at_fmi": {
        "licenseURL":
            "https://ckan.ymparisto.fi/dataset/sentinel-2-image-index-mosaics-s2ind-sentinel-2-kuvamosaiikit-s2ind",
    },
    "sentinel_1_11_days_mosaics_at_fmi": {
        "licenseURL":
            "https://ckan.ymparisto.fi/dataset/sentinel-1-sar-image-mosaic-s1sar-sentinel-1-sar-kuvamosaiikki-s1sar",
    },
    "sentinel_1_daily_mosaics_at_fmi": {
        "licenseURL":
            "https://ckan.ymparisto.fi/dataset/sentinel-1-sar-image-mosaic-s1sar-sentinel-1-sar-kuvamosaiikki-s1sar",
    },
    "sentinel_1_tiles_at_fmi": {
        "licenseURL":
            "https://sentinels.copernicus.eu/documents/247904/690755/Sentinel_Data_Legal_Notice",
    },
    "landsat_yearly_mosaics_at_fmi": {
        "licenseURL":
            "https://ckan.ymparisto.fi/dataset/historical-landsat-satellite-image-mosaics-href-historialliset-landsat-kuvamosaiikit-href",
    },
    "landsat_annual_index_mosaics_at_fmi": {
        "licenseURL":
            "https://ckan.ymparisto.fi/dataset/historical-landsat-image-index-mosaics-hind-historialliset-landsat-kuvaindeksimosaiikit-hind",
    },
    "forest_inventory_at_fmi": {
        "licenseURL":
            None,
    },
    "canopy_height_model_at_fmi": {
        "licenseURL":
            None,
    },
    "2m_digital_terrain_model_products_at_fmi": {
        "licenseURL":
            "https://www.maanmittauslaitos.fi/en/opendata-licence-cc40",
    },
    "forest_wind_damage_risk_at_fmi": {
        "licenseURL":
            None
    },
    "daily_wind_damage_risk_at_fmi": {
        "licenseURL":
            None
    }
}

news_ids = {
    "Sentinel-2_global_mosaic_vuosi": "sentinel_2_annual_mosaics_at_fmi",
    "Sentinel-2_global_mosaic_dekadi": "sentinel_2_11_days_mosiacs_at_fmi",
    "Sentinel-2_indeksimosaiikit": "sentinel_2_monthly_index_mosaics_at_fmi",
    "Sentinel-1_dekadi_mosaiikki": "sentinel_1_11_days_mosaics_at_fmi",
    "Sentinel-1_daily_mosaiikki": "sentinel_1_daily_mosaics_at_fmi",
    "Sentinel-1_osakuvat": "sentinel_1_tiles_at_fmi",
    "Landsat_pintaheijastus": "landsat_yearly_mosaics_at_fmi",
    "Landsat_indeksit": "landsat_annual_index_mosaics_at_fmi",
    "Latvuskorkeusmalli": "canopy_height_model_at_fmi",
    "Metsavarateema": "forest_inventory_at_fmi",
    "MML-DTM-2m": "2m_digital_terrain_model_products_at_fmi",
    "Myrskytuhoriskikartta": "forest_wind_damage_risk_at_fmi",
    "Tuulituhoriski": "daily_wind_damage_risk_at_fmi"
}


def retry_errors(list_of_items, list_of_errors):

    print("TRYING TO ADD ERRORED ITEMS")
    while len(list_of_errors) > 0:
        for i,item in enumerate(list_of_errors):
            try:
                list_of_items.append(pystac.Item.from_file(item))
                print(f"Added item {item}")
                list_of_errors.remove(item)
            except Exception as e:
                print(f"ERROR {e} in item {item} #{i}")

    return 0


def json_convert(content):

    """
        jsonfile: json file in dict format
        
        A function to map the Sentinel-2 STAC jsonfiles into the GeoServer database layout.
        There are different json layouts for Collections and Items. The function checks if the jsonfile is of type "Collection",
        or of type "Feature" (=Item). A number of properties are hardcoded into Sentinel-2 metadata as these are not collected in the STAC jsonfiles.
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
                "derivedFrom": {
                    "href": content["derived_from"],
                    "rel": "derived_from",
                    "type": "application/geo+json"
                }, # Derived_from added
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

    return new_json

def create_catalog(app_host, csc_catalog):

    collections = []
    for collection in fmi_collections:
        try:
            collections.append(Collection.from_file(collection))
        except ValueError:
            with urllib.request.urlopen(collection) as url:
                data = json.load(url)
                data["extent"]["temporal"]["interval"] = [data["extent"]["temporal"]["interval"]]
                collections.append(Collection.from_dict(data))

    for collection in collections:

        collection.id = news_ids[collection.id]
        print(f"Checking collection {collection.id}")
        collection_links = collection.get_child_links()

        sub_collections = []
        for link in collection_links:
            try:
                sub_collections.append(Collection.from_file(link.target))
            except ValueError:
                with urllib.request.urlopen(link.target) as url:
                    data = json.load(url)
                    data["extent"]["temporal"]["interval"] = [data["extent"]["temporal"]["interval"]]
                    sub_collections.append(Collection.from_dict(data))

        item_links = list(set([link.target for sub in sub_collections for link in sub.get_item_links()]))

        csc_collection = csc_catalog.get_collection(collection.id)
        csc_item_ids = [x.id for x in csc_collection.get_items()]

        items = []
        errors = []
        for i,item in enumerate(item_links):
            try:
                items.append(pystac.Item.from_file(item))
            except Exception as e:
                print(f"ERROR {e} in item {item} #{i}")
                errors.append(item)
        print(f" * Number of items in CSC STAC vs FMI for {collection.id}: {len(csc_item_ids)}/{len(items)}")

        # If there were connection errors during the item making process, the item generation for errors is retried
        if len(errors) > 0:
            retry_errors(items, errors)
            print("All errors fixed")

        for i,item in enumerate(items):

            if item.id not in csc_item_ids:

                collection.add_item(item)

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
                # if collection_info[collection.id]["licenseURL"]:
                #     item.add_link(pystac.Link(
                #         rel="license",
                #         target=collection_info[collection.id]["licenseURL"],
                #         title="License"
                #     ))

                item_dict = item.to_dict()

                converted = json_convert(item_dict)

                request_point = f"collections/{collection.id}/products"
                r = requests.post(urljoin(app_host, request_point), json=converted, auth=HTTPBasicAuth("admin", pwd))
                r.raise_for_status()

                print(f" + Added item {item.id} to {collection.id}")

        print(f" * All items present in {collection.id}")

if __name__ == "__main__":

    pwd = getpass.getpass()

    app_host = "http://86.50.229.158:8080/geoserver/rest/oseo/"
    csc_catalog = pystac_client.Client.open("http://86.50.229.158:8080/geoserver/ogc/stac/")

    create_catalog(app_host, csc_catalog)