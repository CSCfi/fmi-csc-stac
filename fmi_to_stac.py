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
        "title":
            "Sentinel-2 annual surface reflectance mosaics.",
        "description": 
            "Sentinel-2 annual surface reflectance  mosaics. Resolution: 10m. Covered area: Finland. Original Sentinel-2 data from ESA Copernicus Sentinel Program, mosaic processing by Sentinel-2 Global Mosaic Service, mosaic postprocessing by SYKE.",
        "metadata":
            "https://ckan.ymparisto.fi/dataset/sentinel-2-satellite-image-mosaics-s2gm-sentinel-2-satelliittikuvamosaiikki-s2gm",
        "licenseURL":
            "https://ckan.ymparisto.fi/dataset/sentinel-2-satellite-image-mosaics-s2gm-sentinel-2-satelliittikuvamosaiikki-s2gm",
        "original_href":
            "https://pta.data.lit.fmi.fi/stac/catalog/Sentinel-2_global_mosaic_vuosi/Sentinel-2_global_mosaic_vuosi.json",
        "license":
            "CC-BY-4.0",
        "providers": [
            pystac.Provider(
                name="FMI",
                url="https://en.ilmatieteenlaitos.fi/",
                roles=[
                    "host"
                ]
            ),
            pystac.Provider(
                name="ESA",
                url="https://www.esa.int/",
                roles=[
                    "producer"
                ]
            ),
            pystac.Provider(
                name="SYKE",
                url="https://www.syke.fi/en-US",
                roles=[
                    "processor",
                    "licensor"
                ]
            ),
            pystac.Provider(
                name="Sentinel-2 Global Mosaic service",
                url="https://s2gm.land.copernicus.eu/",
                roles=[
                    "processor"
                ]
            )
        ]
    },
    "sentinel_2_11_days_mosiacs_at_fmi": {
        "title":
            "Sentinel-2 11-days surface reflectance mosaics.",
        "description":
            "Sentinel-2 11-days surface reflectance mosaics. Resolution: 10m. Covered area: Finland. Original Sentinel-2 data from ESA Copernicus Sentinel Program, mosaic processing by Sentinel-2 Global Mosaic Service. Mosaic postprocessing by SYKE.",
        "metadata":
            "https://ckan.ymparisto.fi/dataset/sentinel-2-satellite-image-mosaics-s2gm-sentinel-2-satelliittikuvamosaiikki-s2gm",
        "licenseURL":
            "https://ckan.ymparisto.fi/dataset/sentinel-2-satellite-image-mosaics-s2gm-sentinel-2-satelliittikuvamosaiikki-s2gm",
        "original_href":
            "https://pta.data.lit.fmi.fi/stac/catalog/Sentinel-2_global_mosaic_dekadi/Sentinel-2_global_mosaic_dekadi.json",
        "license":
            "CC-BY-4.0",
        "providers": [
            pystac.Provider(
                name="FMI",
                url="https://en.ilmatieteenlaitos.fi/",
                roles=[
                    "host"
                ]
            ),
            pystac.Provider(
                name="Sentinel-2 Global Mosaic service",
                url="https://s2gm.land.copernicus.eu/",
                roles=[
                    "processor"
                ]
            ),
            pystac.Provider(
                name="ESA",
                url="https://www.esa.int/",
                roles=[
                    "producer"
                ]
            ),
            pystac.Provider(
                name="SYKE",
                url="https://www.syke.fi/en-US",
                roles=[
                    "processor",
                    "licensor"
                ]
            ),
        ]
    },
    "sentinel_2_monthly_index_mosaics_at_fmi": {
        "title":
            "Sentinel-2 monthly index mosaics: NDVI, NDBI, NDMI, NDSI, NDTI.",
        "description":
            "Sentinel-2 monthly index mosaics: NDVI, NDBI, NDMI, NDSI, NDTI. Resolution: 10m. Covered area: Finland. Available each year for April-October. Original Sentinel-2 data from ESA Copernicus Sentinel Program, processing by SYKE and FMI.",
        "metadata":
            "https://ckan.ymparisto.fi/dataset/sentinel-2-image-index-mosaics-s2ind-sentinel-2-kuvamosaiikit-s2ind",
        "licenseURL":
            "https://ckan.ymparisto.fi/dataset/sentinel-2-image-index-mosaics-s2ind-sentinel-2-kuvamosaiikit-s2ind",
        "original_href":
            "https://pta.data.lit.fmi.fi/stac/catalog/Sentinel-2_indeksimosaiikit/Sentinel-2_indeksimosaiikit.json",
        "license":
            "CC-BY-4.0",
        "providers": [
            pystac.Provider(
                name="FMI",
                url="https://en.ilmatieteenlaitos.fi/",
                roles=[
                    "host",
                    "processor"
                ]
            ),
            pystac.Provider(
                name="ESA",
                url="https://www.esa.int/",
                roles=[
                    "producer"
                ]
            ),
            pystac.Provider(
                name="SYKE",
                url="https://www.syke.fi/en-US",
                roles=[
                    "processor",
                    "licensor"
                ]
            ),
        ]
    },
    "sentinel_1_11_days_mosaics_at_fmi": {
        "title":
            "Sentinel-1 11-days backscatter mosaics: VV and VH polarisation.",
        "description":
            "Sentinel-1 11-days backscatter mosaics: VV and VH polarisation. Resolution: 20m.  Covered area: Finland. Original Sentinel-1 data from ESA Copernicus Sentinel Program, processing by FMI.",
        "metadata":
            "https://ckan.ymparisto.fi/dataset/sentinel-1-sar-image-mosaic-s1sar-sentinel-1-sar-kuvamosaiikki-s1sar",
        "licenseURL":
            "https://ckan.ymparisto.fi/dataset/sentinel-1-sar-image-mosaic-s1sar-sentinel-1-sar-kuvamosaiikki-s1sar",
        "original_href":
            "https://pta.data.lit.fmi.fi/stac/catalog/Sentinel-1_dekadi_mosaiikki/Sentinel-1_dekadi_mosaiikki.json",
        "license":
            "CC-BY-4.0",
        "providers": [
            pystac.Provider(
                name="FMI",
                url="https://en.ilmatieteenlaitos.fi/",
                roles=[
                    "host",
                    "processor",
                    "licensor"
                ]
            ),
            pystac.Provider(
                name="ESA",
                url="https://www.esa.int/",
                roles=[
                    "producer"
                ]
            ),
        ]
    },
    "sentinel_1_daily_mosaics_at_fmi": {
        "title":
            "Sentinel-1 daily backscatter mosaics: VV and VH polarisation.",
        "description":
            "Sentinel-1 daily backscatter mosaics: VV and VH polarisation. Resolution: 20m. Covered area: Finland. Original Sentinel-1 data from ESA Copernicus Sentinel Program, processing by FMI.",
        "metadata":
            "https://ckan.ymparisto.fi/dataset/sentinel-1-sar-image-mosaic-s1sar-sentinel-1-sar-kuvamosaiikki-s1sar",
        "licenseURL":
            "https://ckan.ymparisto.fi/dataset/sentinel-1-sar-image-mosaic-s1sar-sentinel-1-sar-kuvamosaiikki-s1sar",
        "original_href":
            "https://pta.data.lit.fmi.fi/stac/catalog/Sentinel-1_daily_mosaiikki/Sentinel-1_daily_mosaiikki.json",
        "license":
            "CC-BY-4.0",
        "providers": [
            pystac.Provider(
                name="FMI",
                url="https://en.ilmatieteenlaitos.fi/",
                roles=[
                    "host",
                    "processor",
                    "licensor"
                ]
            ),
            pystac.Provider(
                name="ESA",
                url="https://www.esa.int/",
                roles=[
                    "producer"
                ]
            ),
        ]
    },
    "sentinel_1_tiles_at_fmi": {
        "title":
            "Sentinel-1 backscatter tiles: VV and VH polarisation.",
        "description":
            "Sentinel-1 backscatter tiles: VV and VH polarisation. Resolution: 20m. Covered area: Finland. Original Sentinel-1 data from ESA Copernicus Sentinel Program, processing by FMI.",
        "metadata":
            "https://sentinels.copernicus.eu/web/sentinel/user-guides/sentinel-1-sar",
        "licenseURL":
            "https://sentinels.copernicus.eu/documents/247904/690755/Sentinel_Data_Legal_Notice",
        "original_href":
            "https://pta.data.lit.fmi.fi/stac/catalog/Sentinel-1_osakuvat/Sentinel-1_osakuvat.json",
        "license":
            "CC-BY-SA-3.0",
        "providers": [
            pystac.Provider(
                name="FMI",
                url="https://en.ilmatieteenlaitos.fi/",
                roles=[
                    "host",
                    "processor",
                    "licensor"
                ]
            ),
            pystac.Provider(
                name="ESA",
                url="https://www.esa.int/",
                roles=[
                    "producer"
                ]
            ),
        ]
    },
    "landsat_yearly_mosaics_at_fmi": {
        "title":
            "Landsat annual surface reflectance mosaics.",
        "description":
            "Landsat annual surface reflectance mosaics. Resolution: 30m. Covered area: Finland. Avaialble years: 1985, 1990 and 1995. Original Landsat imagery from USGS and ESA, processing by Blom Kartta.",
        "metadata":
            "https://ckan.ymparisto.fi/dataset/historical-landsat-satellite-image-mosaics-href-historialliset-landsat-kuvamosaiikit-href",
        "licenseURL":
            "https://ckan.ymparisto.fi/dataset/historical-landsat-satellite-image-mosaics-href-historialliset-landsat-kuvamosaiikit-href",
        "original_href":
            "https://pta.data.lit.fmi.fi/stac/catalog/Landsat_pintaheijastus/Landsat_pintaheijastus.json",
        "license":
            "CC-BY-4.0",
        "providers": [
            pystac.Provider(
                name="FMI",
                url="https://en.ilmatieteenlaitos.fi/",
                roles=[
                    "host"
                ]
            ),
            pystac.Provider(
                name="SYKE",
                url="https://www.syke.fi/en-US",
                roles=[
                    "licensor"
                ]
            ),
            pystac.Provider(
                name="USGS",
                url="https://www.usgs.gov/",
                roles=[
                    "producer"
                ]
            ),
            pystac.Provider(
                name="Blom Kartta",
                url="https://blomkartta.fi/",
                roles=[
                    "processor"
                ]
            ),
        ]
    },
    "landsat_annual_index_mosaics_at_fmi": {
        "title":
            "Landsat (4 and 5) yearly index mosaics: NDVI, NDBI, NDMI, NDSI, NDTI.",
        "description":
            "Landsat (4 and 5) yearly index mosaics: NDVI, NDBI, NDMI, NDSI, NDTI. Resolution: 30m. Covered area: Finland. Available for the years 1984-2011. Landsat-4/5 imagery from United States Geological Survey. Mosaics processed by SYKE at Finnish National Satellite Data Centre.",
        "metadata":
            "https://ckan.ymparisto.fi/dataset/historical-landsat-image-index-mosaics-hind-historialliset-landsat-kuvaindeksimosaiikit-hind",
        "licenseURL":
            "https://ckan.ymparisto.fi/dataset/historical-landsat-image-index-mosaics-hind-historialliset-landsat-kuvaindeksimosaiikit-hind",
        "original_href":
            "https://pta.data.lit.fmi.fi/stac/catalog/Landsat_indeksit/Landsat_indeksit.json",
        "license":
            "CC-BY-4.0",
        "providers": [
            pystac.Provider(
                name="FMI",
                url="https://en.ilmatieteenlaitos.fi/",
                roles=[
                    "host"
                ]
            ),
            pystac.Provider(
                name="USGS",
                url="https://www.usgs.gov/",
                roles=[
                    "producer"
                ]
            ),
            pystac.Provider(
                name="SYKE",
                url="https://www.syke.fi/en-US",
                roles=[
                    "processor",
                    "licensor"
                ]
            ),
        ]
    },
    "forest_inventory_at_fmi": {
        "title":
            "Multi-source forest inventory products.",
        "description":
            "Multi-source forest inventory products. Resolution: 20m. Covered area: Finland.",
        "metadata":
            "https://www.paikkatietohakemisto.fi/geonetwork/srv/fin/catalog.search#/metadata/0e7ad446-2999-4c94-ad0d-095991d8f80a",
        "licenseURL":
            None,
        "original_href":
            "https://pta.data.lit.fmi.fi/stac/catalog/Metsavarateema/Metsavarateema.json",
        "license":
            "CC-BY-4.0",
        "providers": [
            pystac.Provider(
                name="FMI",
                url="https://en.ilmatieteenlaitos.fi/",
                roles=[
                    "host"
                ]
            ),
            pystac.Provider(
                name="LUKE",
                url="https://www.luke.fi/en",
                roles=[
                    "producer",
                    "licensor"
                ]
            )
        ]
    },
    "canopy_height_model_at_fmi": {
        "title":
            "Canopy height model.",
        "description":
            "Canopy height model. Resolution: 1m. Covered area: Finland.",
        "metadata":
            "https://www.paikkatietohakemisto.fi/geonetwork/srv/eng/catalog.search#/metadata/8f3b883b-a133-4eee-9f5d-bfd042d782bb",
        "licenseURL":
            None,
        "original_href":
            "https://pta.data.lit.fmi.fi/stac/catalog/Latvuskorkeusmalli/Latvuskorkeusmalli.json",
        "license":
            "CC-BY-4.0",
        "providers": [
            pystac.Provider(
                name="FMI",
                url="https://en.ilmatieteenlaitos.fi/",
                roles=[
                    "host"
                ]
            ),
            pystac.Provider(
                name="Finnish Forest Centre",
                url="https://www.metsakeskus.fi/en",
                roles=[
                    "producer",
                    "licensor"
                ]
            )
        ]
    },
    "2m_digital_terrain_model_products_at_fmi": {
        "title":
            "Digital terrain model products: DTM, aspect, slope.",
        "description":
            "Digital terrain model products: DTM, aspect, slope. Resolution: 2m. Covered area: Finland.",
        "metadata":
            "https://www.paikkatietohakemisto.fi/geonetwork/srv/eng/catalog.search#/metadata/053a0a20-abfa-4bf9-ac74-270e845654d1",
        "licenseURL":
            "https://www.maanmittauslaitos.fi/en/opendata-licence-cc40",
        "original_href":
            "https://pta.data.lit.fmi.fi/stac/catalog/MML-DTM-2m/MML-DTM-2m.json",
        "license":
            "CC-BY-4.0",
        "providers": [
            pystac.Provider(
                name="FMI",
                url="https://en.ilmatieteenlaitos.fi/",
                roles=[
                    "host"
                ]
            ),
            pystac.Provider(
                name="NLS",
                url="https://www.maanmittauslaitos.fi/en",
                roles=[
                    "producer",
                    "licensor"
                ]
            )
        ]
    },
    "forest_wind_damage_risk_at_fmi": {
        "title":
            "Forest wind damage risk map.",
        "description":
            "Forest storm damage risk map. Resolution: 16m. Covered area: Finland",
        "metadata":
            "https://metsainfo.luke.fi/fi/cms/tuulituhoriskikartta/tuulituhoriskit-kysymykset",
        "licenseURL":
            None,
        "original_href":
            "https://pta.data.lit.fmi.fi/stac/catalog/Myrskytuhoriskikartta/Myrskytuhoriskikartta.json",
        "license":
            "CC-BY-4.0",
        "providers": [
            pystac.Provider(
                name="FMI",
                url="https://en.ilmatieteenlaitos.fi/",
                roles=[
                    "host"
                ]
            ),
            pystac.Provider(
                name="LUKE",
                url="https://www.luke.fi/en",
                roles=[
                    "producer",
                    "licensor"
                ]
            )
        ]
    },
    "daily_wind_damage_risk_at_fmi": {
        "title":
            "Daily wind damage risk map.",
        "description":
            "Daily wind damage risk map. Resolution: 500m. Covered area: Finland",
        "metadata":
            None,
        "licenseURL":
            None,
        "original_href":
            "https://pta.data.lit.fmi.fi/stac/catalog/Tuulituhoriski/Tuulituhoriski.json",
        "license":
            "CC-BY-4.0",
        "providers": [
            pystac.Provider(
                name="FMI",
                url="https://en.ilmatieteenlaitos.fi/",
                roles=[
                    "host",
                    "producer",
                    "licensor"
                ]
            )
        ]
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


root_catalog = Catalog(id="FMI", description="", catalog_type= pystac.CatalogType.RELATIVE_PUBLISHED)
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
    # sub_collections = [Collection.from_file(link.target) for link in collection_links]
    print(f"Number of subcollections in {collection.id}: {len(sub_collections)}")
    item_links = list(set([link.target for sub in sub_collections for link in sub.get_item_links()]))
    print(f"Number of item links in {collection.id}: {len(item_links)}")
    items = []
    errors = []
    for i,item in enumerate(item_links):
        try:
            items.append(pystac.Item.from_file(item))
        except Exception as e:
            print(f"ERROR {e} in item {item} #{i}")
            errors.append(item)
    print(f"Number of items in {collection.id}: {len(items)}", flush=True)

    # collection.clear_children()
    collection.remove_links("child")
    collection.remove_links("license")

    collection.title = collection_info[collection.id]["title"]
    collection.description = collection_info[collection.id]["description"]
    collection.providers = collection_info[collection.id]["providers"]
    collection.extra_fields["derived_from"] = collection_info[collection.id]["original_href"]
    collection.license = collection_info[collection.id]["license"]
    if collection_info[collection.id]["metadata"]:
        collection.add_link(pystac.Link(
            rel="metadata",
            target=collection_info[collection.id]["metadata"],
            title="Metadata"
        ))
        collection.add_asset(
            key="metadata",
            asset=pystac.Asset(
                href=collection_info[collection.id]["metadata"],
                title="Metadata",
                roles=[
                    "metadata"
                ]
            )
        )
    if collection_info[collection.id]["licenseURL"]:
        collection.add_link(pystac.Link(
            rel="license",
            target=collection_info[collection.id]["licenseURL"],
            title="License"
        ))

    # If there were connection errors during the item making process, the item generation for errors is retried
    if len(errors) > 0:
        retry_errors(items, errors)
        print("All errors fixed")

    for i,item in enumerate(items):

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

        collection.add_item(item)
        print(f"ITEM #{i} added")

    root_catalog.add_child(collection)
    print(f"ITEMS in {collection.id}: {len(items)}")

root_catalog.normalize_and_save('FMI')
print("Catalog normalized and saved")
