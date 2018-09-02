import sys
import os

import process_file


if __name__ == "__main__":

    directory_to_process = sys.argv[1]
    target_table_name = sys.argv[2]
    i = 0
    for file in os.listdir(directory_to_process):
        if file.endswith(".hdf"):
            hdf_file = os.path.join(directory_to_process , file)
            i+=1
            print("processing file {}".format(i))
            process_file.process(hdf_file, target_table_name)
