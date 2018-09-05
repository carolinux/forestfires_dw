import sys
import os

import psycopg2

import process_file
import process_fire


def create_table_for_forest_boxes(target_table_name):
    conn_string = "dbname='fires' user='carolinux'"
    conn = psycopg2.connect(conn_string)
    cur = conn.cursor()
    cur.execute("""CREATE TABLE {}
    (
      box geometry,
      tree_cover double precision,
      year integer
    );
 """.format(target_table_name))
    cur.execute("""
CREATE INDEX {}_geom_idx
  ON {}
  USING gist
  (box);
    """.format(target_table_name, target_table_name))
    conn.commit()
    conn.close()


if __name__ == "__main__":

    year = sys.argv[1]
    directory_to_process = year
    target_table_name = 'forestboxes{}'.format(year)
    i = 0

    create_table_for_forest_boxes(target_table_name)


    for file in os.listdir(directory_to_process):
        if file.endswith(".hdf"):
            hdf_file = os.path.join(directory_to_process , file)
            i+=1
            print("processing file {}".format(i))
            process_file.process(hdf_file, target_table_name)

    print("Converted all hdfs files into latlons")

    # now: join with fire data

    process_fire.process(year)

