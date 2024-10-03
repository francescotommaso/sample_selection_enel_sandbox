import os
import pandas as pd
import json
from misc import log
from misc import log


def save_dictionaries(str_to_int, int_to_str, str_to_int_filename='str_to_int.json',
                      int_to_str_filename='int_to_str.json'):
    with open('data/misc/' + str_to_int_filename, 'w') as file:
        json.dump(str_to_int, file)
    with open('/data/misc/' + int_to_str_filename, 'w') as file:
        json.dump({str(k): v for k, v in int_to_str.items()}, file)


def load_dictionaries(str_to_int_filename='str_to_int.json', int_to_str_filename='int_to_str.json'):
    with open('data/misc/' + str_to_int_filename, 'r') as file:
        str_to_int = json.load(file)
    with open('data/misc/' + int_to_str_filename, 'r') as file:
        int_to_str = {int(k): v for k, v in json.load(file).items()}  # Convert keys back to integers

    return str_to_int, int_to_str


def creating_mappings():
    pods_str_to_int_dict = dict()
    pods_int_to_srt_dict = dict()
    unique_pods_set = set()

    csv_files_path = 'data/recent_measurements/'

    csv_files_list = os.listdir(csv_files_path)
    for csv_file in csv_files_list:
        log('Processing file {0}'.format(csv_file))
        df = pd.read_csv(csv_files_path + csv_file, sep=';')
        meters_list = df['medidor'].to_list()
        for meter in meters_list:
            unique_pods_set.add(meter)
        log('Finished processing file {0}'.format(csv_file))

    integer_count = 0

    for pod in unique_pods_set:
        if pod not in pods_str_to_int_dict:
            pods_str_to_int_dict[pod] = integer_count
            pods_int_to_srt_dict[integer_count] = pod
            integer_count += 1

    save_dictionaries(str_to_int=pods_str_to_int_dict, int_to_str=pods_int_to_srt_dict)


creating_mappings()


