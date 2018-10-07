# How to prepare country shapefiles for querying

- Donwload all country shapefiles from: https://www.naturalearthdata.com/downloads/50m-cultural-vectors/50m-admin-0-countries-2/
- Load into postgresql
```
shp2pgsql -s 4326 countries/ne_50m_admin_0_countries.shp countries postgres > countries.sql
psql -d fires -f countries.sql
```

- Split the countries in pieces to speed up querying
```
create table countries_split as (select name, iso_a2, st_subdivide(geom, 8) from countries);
CREATE INDEX countries_split_geom_idx
  ON countries_split
  USING gist
  (st_subdivide);
```

- Correct missing iso_a2 in Norway and France!

```
UPDATE countries_split SET iso_a2 = 'FR' WHERE name = 'France';
UPDATE countries SET iso_a2 = 'FR' WHERE name = 'France';
UPDATE countries_split SET iso_a2 = 'NO' WHERE name = 'Norway';
UPDATE countries SET iso_a2 = 'NO' WHERE name = 'Norway';
```

- Now the tables are ready to query with process_area.py

