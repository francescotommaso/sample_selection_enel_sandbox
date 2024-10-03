import pandas as pd
import os
from hdf5_mappings import load_dictionaries
from misc import log


def read_and_clean_csv(file_path):
    # Read CSV file
    df = pd.read_csv(file_path, index_col=None, header=0, sep=';', low_memory=False)
    trimmed_df = df.drop(['ubicacion', 'tip_lectura'], axis=1)
    
    # Convert 'medidor' strings to integers using the dictionary
    medidor_str_to_int_dict, _ = load_dictionaries()
    trimmed_df['medidor'] = trimmed_df['medidor'].map(medidor_str_to_int_dict).fillna(-1).astype(int)
    
    # Convert 'fec_interval' to datetime
    trimmed_df['fec_interval'] = pd.to_datetime(trimmed_df['fec_interval'])
    
    # Remove rows with non-round hours (minutes or seconds different from 0)
    trimmed_df = trimmed_df[trimmed_df['fec_interval'].dt.minute == 0]
    trimmed_df = trimmed_df[trimmed_df['fec_interval'].dt.second == 0]
    
    # Replace non-integers or negative consumption values with zero
    trimmed_df['consumo'] = pd.to_numeric(trimmed_df['consumo'], errors='coerce').fillna(0)
    trimmed_df.loc[trimmed_df['consumo'] < 0, 'consumo'] = 0
    
    return trimmed_df


def clean_and_save_csv(csv_folder):    
    # Read and clean all CSV files first to initialize datasets
    csv_files = [os.path.join(csv_folder, f) for f in os.listdir(csv_folder) if f.endswith('.csv')]
    csv_files.sort()
    csv_count = 0
    number_of_csv = len(csv_files)
    for csv_file in csv_files:
        log(f'Opening and cleaning {os.path.basename(csv_file)}...')
        df = read_and_clean_csv(csv_file)
        log(f'{os.path.basename(csv_file)} was parsed and cleaned. It will be saved now...')
        df.to_csv('data/recent_measurements_clean/{}'.format(os.path.basename(csv_file)), index=False)
        csv_count += 1
        log('The dataframe was saved successfully. {0} of {1} done!'.format(csv_count, number_of_csv))        


# Set the folder containing CSV files
csv_folder = 'data/recent_measurements/'
clean_and_save_csv(csv_folder)



