Data structure:

Inputs (required as data source):

Hourly measurements (january to april) of advanced meters. CSV files (121). Source: ENEL:
data/recent_measurements/{csv_file}

Monthly consumption (2023) for consumers with advanced meters. CSV file. Source: ENEL:
data/complementary/Base_SM.csv

BDGD data (shapefiles) (for 2023) with data for consumption for all consumers in ENEL. Source: ENEL:
data/UCBT/{shapefile}

Census data for SP (2010):
data/sp_setores_censitarios_2010/35SEE250GC_SIR.shp
data/SP_Capital_20231030/Base informaçoes setores2010 universo SP_Capital/CSV/DomicilioRenda_SP1_adj.csv
data/SP_Exceto_Capital_20231030/Base informaçoes setores2010 universo SP_Exceto_Capital/CSV/DomicilioRenda_SP2_adj.csv

Atypical consumers for ENEL SP (low income, home care, distributed generation, and so on)
data/complementary/atipicos.xlsx



Outputs (directories structure required for intermediary and final outputs). Make sure the following, empty directories, exist:

Meters to integers and integers to meters dictionaries (for HDF5 reference):
Origin -> hdf5_mappings.py
data/misc/

Hourly measurements clean:
Origin -> clean_csv_data.py
data/recent_measurements_clean/

HDF5, memory efficient data:
data/hdf5

Consolidated consumption data:
Origin -> data_consolidation.py
data/consolidated/

Samples results:
Origin -> sample_selection.py
data/samples/


Step by step procedure:

1. Prepare basic data structure:
A. Read data_structure.txt and place data accordingly


2. Create hdf5 mapping:
What is does:
A. Creates a mapping between integers and meter codes (serial numbers), in order to create efficient hdf5 file.
How to run:
hdf5_mappings.py


3. Clean csv files with ourly consumption data:
What is does:
A. Removes unnecessary columns: The columns 'ubicacion' and 'tip_lectura' are dropped from the dataframe.
B. Converts the 'fec_interval' column to datetime: Ensures that the 'fec_interval' column is in a proper datetime format.
C. Filters the dataframe: Keeps only rows where the 'fec_interval' values have a minute and second equal to 0 (i.e., only keeps rows with round-hour timestamps).
D. Cleans the 'consumo' column: Converts the 'consumo' column to numeric values, replacing any non-numeric entries with 0. It also sets any negative consumption values to 0.
E. Creates a clean copy of the csv data in the dir 'data/recent_measurements_clean'
How to run:
clean_csv_data.py


4. HDF5 measurements file creation:
What is does:
Creates a file in HDF5 format of the measurements. The final size of the data is approximately 20 times smaller than on csv.
How to run:

On Linux:
"sudo apt-get install libhdf5-dev libhdf5-cpp-100"
"sudo apt-get install g++"
The compile with "g++ -O3 -march=native -std=c++11 -o csv_to_hdf5 csv_to_hdf5.cpp -I/usr/include/hdf5/serial -L/usr/lib/x86_64-linux-gnu/hdf5/serial -lhdf5_cpp -lhdf5 -lz -ldl -lm"
Then run "./csv_to_hdf5"

On Windows:
Install MinGW-w64 (or a similar compiler)
Install HDF5 Precompiled Binaries
Extract the downloaded files, and note the installation directory (e.g., C:\HDF5).
Set Up Environment Variables
C:\HDF5\include to the INCLUDE variable.
C:\HDF5\lib to the LIB variable.
Add C:\HDF5\bin to the Path variable for runtime.
Compile using MinGW-w64 -> "g++ -O3 -std=c++11 -o csv_to_hdf5 csv_to_hdf5.cpp -IC:/HDF5/include -LC:/HDF5/lib -lhdf5_cpp -lhdf5 -lz -lm"
Then run "csv_to_hdf5.exe"


5. HDF5 consumption file creation:
What is does:
Creates a file in HDF5 format of the consumption.
How to run:
Compile create_consumptions_hdf5.cpp following the steps above (item 4) and run.


6. Base ENEL SP data consolidation:
What is does:
Consolidates several data sources into one single file called "enel_march_income_iicc_ene.csv", which has data for average income, iicc (for march) and average energy consumption. It deals with data gaps and errors as well.
How to run:
data_consolidation.py


7. Make samples selection:
What is does:
Selects the random samples for each strata and saves them as xlsx files.
How to run:
sample_selection.py


