# Forest Fires Project

This project contains the code that is used to download and process satellite data about forest areas of the world.

## Dependencies

- Install the python deps from `requirements.txt`
- Install postgresql and postgis. Create a database named `fires`
- Change the user from `carolinux` to your user in all the python files in this project
- Create a folder fire_csvs with your csvs (named as df_cleaned_year.csv)
- You also need `gdalwarp` in the path

## Compute fires-in-forest areas dataset

- Get an account on the MODIS website to be able to donwload the granules
- Run:
```
python main.py year modis_username modis_password
```

## Compute forest areas-per-country

- Install `shp2pgsql`
- Go through the steps in `README_forest_coverage.md`
- Run:
```
python process_area.py year
```