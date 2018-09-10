import os
from datetime import datetime

import pandas as pd
import psycopg2


def process(year):
    table_name = 'fires{}'.format(year)
    boxes_table_name = 'forestboxes{}'.format(year)
    forest_table_name = 'forest' + table_name
    fn = "fire_csvs/df_cleaned_{}.csv".format(year)
    df = pd.read_csv(fn)
    df.rename(columns={df.columns[0]: "index"}, inplace=True)
    conn_string = "dbname='fires' user='carolinux'"
    conn = psycopg2.connect(conn_string)
    cur = conn.cursor()
    cur.execute("""CREATE TABLE {}
(
  fire_pt geometry,
  index integer
);""".format(table_name))
    cur.execute("""
CREATE INDEX {}_geom_idx
  ON {}
  USING gist
  (fire_pt);""".format(table_name, table_name))

    for _, values in df.iterrows():
        sql = 'INSERT INTO {} (fire_pt, index) VALUES (st_setsrid(st_makepoint({}, {}),4326), {})'.format(table_name,
                                                                                                          values[
                                                                                                              'longitude'],
                                                                                                          values[
                                                                                                              'latitude'],
                                                                                                          values[
                                                                                                              'index'])
        cur.execute(sql)
    conn.commit()
    print('doing the join at {}'.format(datetime.now()))
    sql_join = 'create table {} as (select index, fire_pt from {} join {} on st_coveredby(fire_pt, box))'.format(
        forest_table_name, boxes_table_name, table_name)
    cur.execute(sql_join)
    conn.commit()
    print('join finished at {}'.format(datetime.now()))
    import pandas.io.sql as sqlio
    dat = sqlio.read_sql_query('select index from ' + forest_table_name, conn)
    ids = set(dat.values[:, 0])
    df['fire_started_in_forest_area'] = df['index'].apply(lambda x: x in ids)

    if not os.path.exists('results'):
        os.mkdir('results')
    df.to_csv('results/{}.csv'.format(year), index=False)
