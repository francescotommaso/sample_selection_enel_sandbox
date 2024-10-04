import pandas as pd
import numpy as np
import random
import os


# Categorize consumption
def categorize_consumption(consumption, thresholds):
    if consumption <= thresholds[0]:
        return 'Low Consumption'
    elif consumption <= thresholds[1]:
        return 'Medium Consumption'
    else:
        return 'High Consumption'
    

# Categorize iicc
def categorize_iicc(iicc, thresholds):
    if iicc <= thresholds[0]:
        return 'Low iicc'
    elif iicc <= thresholds[1]:
        return 'Medium iicc'
    else:
        return 'High iicc'
    

# Test scenario to validate the distribution of installations
def test_groups(merged_dir, group_dirs):
    # Expected group sizes
    expected_group_sizes = [100, 170, 170, 280, 280]
    
    # Iterate through each file in the merged directory and check group creation
    for merged_file in os.listdir(merged_dir):
        if merged_file.endswith("_sample.xlsx"):
            # Read the merged file
            df_merged = pd.read_excel(os.path.join(merged_dir, merged_file))
            base_filename = merged_file.replace("_sample.xlsx", "")
            
            merged_instalacoes = set(df_merged['Instalation'])
            
            # Check that groups have unique installations
            group_installations = set()
            total_group_installations = 0
            for i, group_dir in enumerate(group_dirs):
                group_file = f"{base_filename}_group_{i + 1}_sample.xlsx"
                df_group = pd.read_excel(os.path.join(group_dir, group_file))
                
                # Ensure each group has the expected number of installations
                expected_size = expected_group_sizes[i]
                assert len(df_group) == expected_size, f"Group {i + 1} for {base_filename} does not have {expected_size} installations"
                
                # Ensure no duplicates within the group
                group_instalacoes = set(df_group['Instalation'])
                assert len(group_instalacoes) == expected_size, f"Duplicates found within group {i + 1} for {base_filename}"
                
                # Ensure group installations do not overlap with other groups
                assert group_installations.isdisjoint(group_instalacoes), \
                    f"Group {i + 1} for {base_filename} has overlapping installations with other groups"
                
                # Update the total group installations
                group_installations.update(group_instalacoes)
                total_group_installations += expected_size
            
            # Ensure that the total installations in all groups match the merged installations
            assert total_group_installations == len(merged_instalacoes), \
                f"The total number of installations across groups does not match the merged installations for {base_filename}"
    
    print("All tests passed.")


df = pd.read_csv('data/consolidated/enel_north_march_income_iicc_ene.csv',
                 dtype={'A_instalacao': str, 'serialnumber': str, 'B_instalacao': str, 'SECTOR': str})


# Load the census data from both files and merge them
df_censo_cap = pd.read_csv('data/SP_Capital_20231030/Base informaçoes setores2010 universo SP_Capital/CSV/DomicilioRenda_SP1_adj.csv', 
                        dtype={'Cod_setor': str, 'renda_media': float},
                        sep=';')

df_censo_no_cap = pd.read_csv('data/SP_Exceto_Capital_20231030/Base informaçoes setores2010 universo SP_Exceto_Capital/CSV/DomicilioRenda_SP2_adj.csv', 
                            dtype={'Cod_setor': str, 'renda_media': float},
                            sep=';')

# Merge the two DataFrames
df_censo = pd.concat([df_censo_cap, df_censo_no_cap])

# Drop rows where 'renda_media' is 0
df_censo = df_censo[df_censo['renda_media'] != 0]

# Merge df_2 and df_censo based on the matching column (SECTOR in df_2, Cod_setor in df_censo)
merged_df = pd.merge(df, df_censo, left_on='SECTOR', right_on='Cod_setor', how='left')

# Convert the necessary columns to numeric (use errors='coerce' to convert invalid strings to NaN)
cols_to_convert = ['V008', 'V009', 'V010', 'V011', 'V012', 'V013']
merged_df[cols_to_convert] = merged_df[cols_to_convert].apply(pd.to_numeric, errors='coerce')

# Drop rows where any of the V008 to V013 columns contain NaN values
merged_df = merged_df.dropna(subset=cols_to_convert)

# Calculate the prob_br and prob_nbr columns
merged_df['prob_br'] = merged_df['V008'] / (merged_df['V008'] + merged_df['V009'] + merged_df['V010'] + merged_df['V011'] + merged_df['V012'] + merged_df['V013'])
merged_df['prob_nbr'] = 1 - merged_df['prob_br']


# Recalculate the consumption and iicc thresholds
consumption_thresholds = np.quantile(merged_df['average_consumption'], [0.33, 0.66])
iicc_thresholds = np.quantile(merged_df['iicc'], [0.33, 0.66])

# Apply the consumption and iicc categorization using .loc to avoid SettingWithCopyWarning
merged_df.loc[:, 'consumption_category'] = merged_df['average_consumption'].apply(categorize_consumption, thresholds=consumption_thresholds)
merged_df.loc[:, 'iicc_category'] = merged_df['iicc'].apply(categorize_iicc, thresholds=iicc_thresholds)

# Create the clusters combining consumption and iicc categories
merged_df.loc[:, 'cluster'] = merged_df['consumption_category'] + ' & ' + merged_df['iicc_category']

# Create the dictionary with 'A_instalacao' as the key, and (prob_br, prob_nbr) as the value
instalacao_dict = merged_df.set_index('A_instalacao')[['prob_br', 'prob_nbr']].to_dict('index')

# Create 18 empty sets, one for each of the 9 clusters split into prob_br and prob_nbr
cluster_names = merged_df['cluster'].unique()
empty_sets = {f'{cluster} & Low Income': set() for cluster in cluster_names}
empty_sets.update({f'{cluster} & Non Low Income': set() for cluster in cluster_names})

print(f'Empty sets: {empty_sets}')
print(f'Length of empty sets: {len(empty_sets)}')

# Create supersets for each of the 9 clusters (without income reference)
supersets = {f'{cluster}': set(merged_df[merged_df['cluster'] == cluster]['A_instalacao']) for cluster in cluster_names}

# Simple filter threshold for first filtering
simple_filter_threshold = 0.5

# Initialize the new dictionary of sets similar to empty_sets
pool_sets = {f'{cluster} & Low Income': set() for cluster in cluster_names}
pool_sets.update({f'{cluster} & Non Low Income': set() for cluster in cluster_names})

# Populate the sets based on the condition of prob_br and prob_nbr
for cluster in cluster_names:
    for a_instalacao in supersets[cluster]:
        prob_br = instalacao_dict[a_instalacao]['prob_br']
        prob_nbr = instalacao_dict[a_instalacao]['prob_nbr']
        
        # Add to the 'Low Income' set if prob_br >= simple_filter_threshold
        if prob_br >= simple_filter_threshold:
            pool_sets[f'{cluster} & Low Income'].add(a_instalacao)
        
        # Add to the 'Non Low Income' set if prob_nbr > simple_filter_threshold
        if prob_nbr > simple_filter_threshold:
            pool_sets[f'{cluster} & Non Low Income'].add(a_instalacao)

# Set the random seed for reproducibility, based on ENEL SP concession code on SPARTA
random.seed(148)

# Set the target number for sampling per cluster and group
target_number = 1000

# Group sizes as per the new requirements
group_sizes = [100, 170, 170, 280, 280]

# Create a set to track all sampled installations across clusters
all_sampled_instalacoes = set()

# Directory paths
merged_dir = "data/samples/merged/"
group_dirs = [f"data/samples/group_{i}/" for i in range(1, 6)]

# Create directories for the groups if they don't exist
for group_dir in group_dirs:
    os.makedirs(group_dir, exist_ok=True)

# Iterate over each cluster set to perform sampling and save to CSV and XLSX
for set_name in empty_sets.keys():
    # Get the corresponding pool set
    pool = pool_sets[set_name]
    
    # Randomly sample up to target_number from the pool
    sample_size = min(target_number, len(pool))
    sampled_instalacoes = random.sample(list(pool), sample_size)
    
    # Check if any of the sampled installations are already in all_sampled_instalacoes
    duplicates = set(sampled_instalacoes).intersection(all_sampled_instalacoes)
    if duplicates:
        print(f"Warning: The following installations are duplicated across clusters: {duplicates}")
    else:
        # Add the sampled installations to the global set
        all_sampled_instalacoes.update(sampled_instalacoes)
    
    # Create a DataFrame with the sampled installations and numeration
    df_merged = pd.DataFrame({
        'Numeration': range(1, sample_size + 1),
        'Instalation': sampled_instalacoes
    })
    
    # Clean up set_name to create a valid filename
    base_filename = set_name.replace(' ', '_').replace('&', 'and')
    
    # Save merged group to XLSX
    xlsx_filename_merged = f"{merged_dir}{base_filename}_sample.xlsx"
    df_merged.to_excel(xlsx_filename_merged, index=False)
    print(f"Saved merged sample for '{set_name}' to '{xlsx_filename_merged}'")

    # Split into the specified group sizes
    start_idx = 0
    for i, group_size in enumerate(group_sizes):
        end_idx = min(start_idx + group_size, sample_size)
        
        # Ensure unique installations for each group
        group_installations = sampled_instalacoes[start_idx:end_idx]
        
        # Create a DataFrame for each group
        df_group = pd.DataFrame({
            'Numeration': range(1, len(group_installations) + 1),
            'Instalation': group_installations
        })
        
        # Save each group to its corresponding directory
        xlsx_filename_group = f"{group_dirs[i]}{base_filename}_group_{i + 1}_sample.xlsx"
        df_group.to_excel(xlsx_filename_group, index=False)
        print(f"Saved group {i + 1} sample for '{set_name}' to '{xlsx_filename_group}'")
        
        # Update the start index for the next group
        start_idx = end_idx

# Run the test scenario
test_groups(merged_dir, group_dirs)


