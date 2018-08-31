from osgeo import gdal
import psycopg2
from psycopg2.extras import execute_values
import pandas as pd

import struct
import sys
import subprocess


def process(hdf_file, target_table_name):
    conn_string = "dbname='fires' user='carolinux'"
    conn = psycopg2.connect(conn_string)
    cur = conn.cursor()
    dest_tif_file = hdf_file + '.tif'
    # change the projection to longitude and latitude
    cmd = 'gdalwarp -overwrite -t_srs EPSG:4326 -dstnodata -200 -of GTiff "HDF4_EOS:EOS_GRID:\"{}\":MOD44B_250m_GRID:Percent_Tree_Cover" {}'.format(
        hdf_file, dest_tif_file)
    subprocess.call(cmd, shell=True)
    year = 2017  # FIXME
    # extract box coordinates for every 250x250m tree covered box
    dataset = gdal.Open(dest_tif_file)
    geotransform = dataset.GetGeoTransform()
    band = dataset.GetRasterBand(1)
    band_types_map = {'Byte': 'B', 'UInt16': 'H', 'Int16': 'h', 'UInt32': 'I',
                      'Int32': 'i', 'Float32': 'f', 'Float64': 'd'}
    band_type = gdal.GetDataTypeName(band.DataType)
    topleftX = geotransform[0]  # top left x
    topleftY = geotransform[3]  # top left y
    X = topleftX
    Y = topleftY
    stepX = geotransform[1]
    stepY = geotransform[5]
    num_pixels = band.YSize * band.XSize
    tuples = []
    i = 0
    inserted = 0
    for y in range(band.YSize):

        scanline = band.ReadRaster(0, y, band.XSize, 1, band.XSize, 1, band.DataType)
        values = struct.unpack(band_types_map[band_type] * band.XSize, scanline)
        for value in values:
            i += 1

            if (value >= 30 and value != 200):
                insert_sql = " INSERT INTO {} (box, year, tree_cover) VALUES %s"
                ewkt = "SRID=4326;POLYGON(({} {}, {} {}, {} {},{} {}, {} {}))".format(
                    target_table_name,
                    X, Y,
                    X + stepX, Y,
                    X + stepX, Y + stepY,
                    X, Y + stepY,
                    X, Y)
                tup = (ewkt, year, value)
                tuples.append(tup)
                if i % 100000 == 0:
                    if i % 1000000:
                        print('{} out of {} ({} %)'.format(i, num_pixels, 100.0 * i / num_pixels))
                    execute_values(cur, insert_sql, tuples)
                    conn.commit()
                    inserted += len(tuples)
                    tuples = []

            X += stepX
        X = topleftX
        Y += stepY
    if len(tuples) > 0:  # still have some rows to insert
        execute_values(cur, insert_sql, tuples)
        inserted += len(tuples)
        conn.commit()
    cur.close()
    conn.close()
    print("Found {} forest boxes".format(inserted))
