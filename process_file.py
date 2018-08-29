from osgeo import gdal
import psycopg2
from psycopg2.extras import execute_values
import pandas as pd

import struct
import sys
import subprocess

hdf_file = sys.argv[1]
dest_tif_file = hdf_file + '.tif'

# change the projection to longitude and latitude
cmd = 'gdalwarp -overwrite -t_srs EPSG:4326 -dstnodata -200 -of GTiff "HDF4_EOS:EOS_GRID:\"{}\":MOD44B_250m_GRID:Percent_Tree_Cover" {}'.format(hdf_file, dest_tif_file)

subprocess.call(cmd, shell=True)
year = 2017 # FIXME
# extract box coordinates for every 250x250m tree covered box
dataset = gdal.Open(dest_tif_file)
geotransform = dataset.GetGeoTransform()
band = dataset.GetRasterBand(1)
band_types_map = {'Byte':'B', 'UInt16':'H', 'Int16':'h', 'UInt32':'I', 
            'Int32':'i', 'Float32':'f', 'Float64':'d'}
band_type = gdal.GetDataTypeName(band.DataType)

topleftX = geotransform[0] #top left x
topleftY = geotransform[3] #top left y
X = topleftX
Y = topleftY
stepX = geotransform[1]
stepY = geotransform[5]
num_pixels = band.YSize * band.XSize

tuples = []

i = 0
for y in range(band.YSize):

    scanline = band.ReadRaster(0, 
                               y, 
                               band.XSize, 
                               1, 
                               band.XSize, 
                               1, 
                               band.DataType)

    values = struct.unpack(band_types_map[band_type] * band.XSize, scanline)

    for value in values:
        i+=1

        if(value >= 30 and value!=200):       
            lon_topleft = X
            lat_topleft = Y
            lon_bottomright = X + stepX
            lat_bottomright = Y + stepY
            min_lon = min(lon_topleft, lon_bottomright)
            min_lat = min(lat_topleft, lat_bottomright)
            max_lon = max(lon_topleft, lon_bottomright)
            max_lat = max(lat_topleft, lat_bottomright)
            insert_sql = '''
            INSERT INTO forest_boxes (box, year, tree_cover)
            VALUES (ST_MakeEnvelope({}, {}, {}, {}, 4326), {}, {});'''.\
                    format(min_lon, min_lat, max_lon, max_lat, year, value)
            #cur.execute(insert_sql)
            ewkt = "SRID=4326;POLYGON(({} {}, {} {}, {} {},{} {}, {} {}))".format(
                    X, Y,
                    X + stepX, Y,
                    X + stepX, Y + stepY,
                    X, Y + stepY,
                    X, Y)
            tup = (ewkt, year, value)
            tuples.append(tup)
            if i%100000 == 0:
                print('{} out of {} ({} %)'.format(i, num_pixels, 100.0 * i/num_pixels))

        X += stepX
    X = topleftX
    Y += stepY
df = pd.DataFrame.from_records(tuples)
df.to_csv("/tmp/foo.txt", sep="|", index=False, header=False)
f = open('/tmp/foo.txt')
conn_string = "dbname='fires' user='carolinux'"
conn = psycopg2.connect(conn_string)
cur = conn.cursor()
cur.copy_from(f, 'forest_boxes', columns=('box', 'year', 'tree_cover'), sep="|")
conn.commit()
cur.close()
conn.close()
