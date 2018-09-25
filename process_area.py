import os
from datetime import datetime
import sys

import pandas as pd
import psycopg2


def process(year):
    table_name = 'forestareas{}'.format(year)
    boxes_table_name = 'forestboxes{}'.format(year)



    conn_string = "dbname='fires' user='carolinux'"
    conn = psycopg2.connect(conn_string)
    cur = conn.cursor()
    print('Creating forestareas table at {}'.format(datetime.now()))
    cur.execute("""Create table if not exists {} AS
     (select iso_a2, SUM(ST_Area(ST_Intersection(st_subdivide,box)::geography)/1000000) AS forests_sqkm from countries_split
      join {} on ST_Intersects(st_subdivide, box) group by iso_a2);""".format(table_name, boxes_table_name))
    conn.commit()
    print('Creating forestareas table finished at {}'.format(datetime.now()))


    fn = "fire_csvs/df_cleaned_{}.csv".format(year)
    df = pd.read_csv(fn)
    df["country"] = df.apply(lambda x: x["country"] if x["full_names"] != "Namibia" else "NA", axis=1)

    countries = df[["country", "full_names"]].drop_duplicates()
    countries["valid"] = countries.country.apply(lambda x: x is not None and len(x) == 2)
    countries = countries[countries.valid].drop_duplicates()

    forest_df = pd.read_sql("select iso_a2, SUM(forests_sqkm) AS forests_sqkm from {} where iso_a2!='-99' group by iso_a2".format(table_name), conn)
    countries_df = pd.read_sql("select iso_a2, ST_AREA(geom::geography)/1000000 AS total_sqkm from countries where iso_a2!='-99' ", conn)


    previous_len = len(countries)
    print("Need the forest areas for {} countries".format(previous_len))

    countries.rename(columns={"country": "iso_a2", "full_names": "full_name"}, inplace=True)
    del countries['valid']
    countries['year'] = year



    countries.set_index("iso_a2", inplace=True)
    forest_df.set_index("iso_a2", inplace=True)
    countries_df.set_index("iso_a2", inplace=True)
    joined = countries.join(forest_df).join(countries_df)
    pd.set_option('display.float_format', lambda x: '%.3f' % x)
    if year not in ['2017', '2018']:
        # we get double the data
        joined['forests_sqkm'] = joined['forests_sqkm'] * 0.5

    print("These rows cannot be matched with the country area data {}".format(joined[joined['total_sqkm'].isnull()]))
    joined.dropna(subset=['total_sqkm'], inplace=True)
    joined['forests_sqkm']= joined['forests_sqkm'].fillna(0)
    joined.sort_index(inplace=True)
    joined.index.name = 'iso_a2'
    joined['percentage_forest_cover'] = 100 * joined['forests_sqkm'] / joined['total_sqkm']


    if not os.path.exists('results'):
        os.mkdir('results')
    joined.to_csv('results/forestareas{}.csv'.format(year))
    conn.close()



if __name__ == '__main__':
    year = sys.argv[1]
    process(year)