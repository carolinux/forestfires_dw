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



def download_all_the_files(downloads_file, target_directory, username, password):
    if not os.path.exists(target_directory):
        os.mkdir(target_directory)
    with open(downloads_file, 'r') as f:
        for i, line in enumerate(f):
            url = line.strip()
            target_file = os.path.join(target_directory, '{}.hdf'.format(i))
            if os.path.exists(target_file) and os.path.getsize(target_file)>0:
                continue
            else:
                command = "wget -O {} --retry-connrefused --tries=20 --user {} --password {} {}".format(target_file, username, password, url)
                os.system(command)



def sanity_check(year, directory):
    """check if we have all the files!"""
    if year == '2017':
        expected = 291
    else:
        expected = 582

    hdf_files = [fn for fn in os.listdir(directory) if fn.endswith('hdf')]
    assert len(hdf_files) == expected


if __name__ == "__main__":

    year = sys.argv[1]
    username = sys.argv[2] # the username for NASA website
    pw = sys.argv[3] # the password for NASA website
    #db_user = sys.argv[4] # the db user
    downloads_file = 'granules/{}.txt'.format(year)

    directory_to_process = year
    download_all_the_files(downloads_file, directory_to_process, username, password)
    sanity_check(year, directory_to_process)


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

