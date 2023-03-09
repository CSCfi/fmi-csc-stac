# FMI STAC files to CSC STAC API

These are scripts to add the FMI's static STAC files into the CSC's STAC API

To turn the FMI's static STAC files into a local STAC Catalog, run:
```
python fmi_to_stac.py
```

To upload this local STAC Catalog to GeoServer STAC API, run `python fmi_to_stac.py`. The script prompts to enter the GeoServer password which is needed to run the script:
```
python fmi_to_geoserver.py
Password: <Type password here>
```

The update script is these above two scripts combined without needing to save the STAC Catalog locally. To run the update script, you need to provide the GeoServer password:
```
python update_fmi.py
Password: <Type password here>
```
