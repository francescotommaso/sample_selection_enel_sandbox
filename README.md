# Seleção de Amostra ENEL

### Todos os arquivos necessários estão no Google Drive:
https://drive.google.com/drive/folders/1-BZ1XkxEed1XD6x2veJk3TdDF3Aw_Izz?usp=sharing

Lista de arquivos coms seus respectivos Hash (SHA256):

1. SP_Exceto_Capital_20231030.zip
   - Hash: 0331e2e5413f5cf5812851104dda5a45e19754169e2b77120692d48a64fdc222

2. samples.zip
   - Hash: 0bbe123229a24fd6d1ca62506fa6e9af75f5fe75ed15fb7df9975a0022b1ae38

3. sp_setores_censitarios_2010.zip
   - Hash: 7c5238e122cefa26f7e8817097d637da8ab9c2c94e5f5a7e66c2222ec7e3f789

4. complementary.zip
   - Hash: f53bc06b36a321ec97afe6c7da6dd94ea6cda27a5303d6b5fc96a23b8b60c5e7

5. SP_Capital_20231030.zip
   - Hash: c5253fd5bcef2d31e206165fc337ddb91e4e49acb5c55a4c253eb728d08e2401

6. UCBT.zip
   - Hash: fd80ffc5888bec362a594bba803d502d2f83ff4bdfe00b45c031f4776edf2f71

7. recent_measurements.zip
   - Hash: 55010c936ae2580b413bf52342f1ae03b94d576f36206d4a473df22759082e55

## Estrutura de Dados

### Dados de Entrada (Dados Requisitos)

1. **Medição Horária (Janeiro a Abril) dos Medidores Avançados**
   - **Fonte**: ENEL  
   - **Arquivso**: 121 arquivos CSV  
   - **Instruções**:  
     Extrair o arquivo `recent_measurements.zip` do Google Drive e posicionar no diretório `data/recent_measurements/`.  
     - Estrutura:  
       `data/recent_measurements/{csv_file}`

2. **Consumo Mensal (2023) para Consumidores com Medidores Avançados e Listas Atípicas**
   - **Fonte**: ENEL  
   - **Arquivos**: CSV, XLSX  
   - **Instruções**:  
     Extraia o arquivo `complementary.zip` do Google Drive e coloque-o no diretório `data/complementary/`.  
     - Estrutura dos dados:  
       - `data/complementary/Base_SM.csv`  
       - `data/complementary/atipicos.xlsx`  
       - `data/complementary/baixa_renda.xlsx`

3. **Dados BDGD (Shapefiles) para Consumo (2023)**
   - **Fonte**: ENEL  
   - **Arquivos**: Shapefiles  
   - **Instruções**:  
     Extraia o arquivo `UCBT.zip` do Google Drive e coloque-o no diretório `data/UCBT/`.  
     - Estrutura dos dados:  
       `data/UCBT/{shapefile}`

4. **Dados do Censo de SP (2010)**
   - **Fonte**: IBGE  
   - **Arquivos**: Arquivos ZIP  
   - **Instruções**:  
     Extraia os arquivos `SP_Capital_20231030.zip`, `SP_Exceto_Capital_20231030.zip`, e `sp_setores_censitarios_2010.zip` do Google Drive e coloque-os nos diretórios apropriados.  
     - Estrutura dos dados:  
       - `data/sp_setores_censitarios_2010/{...}`  
       - `data/SP_Capital_20231030/{...}`  
       - `data/SP_Exceto_Capital_20231030/{...}`

### Saídas (Estrutura de Diretórios para Saídas Intermediárias e Finais)

Certifique-se de que os seguintes diretórios vazios existam:

1. **Dicionários de Medidores para Inteiros e Inteiros para Medidores**
   - **Fonte**: `hdf5_mappings.py`  
   - **Diretório**: `data/misc/`

2. **Medições Horárias Limpas**
   - **Fonte**: `clean_csv_data.py`  
   - **Diretório**: `data/recent_measurements_clean/`

3. **Dados HDF5 (Eficiente em Memória)**
   - **Diretório**: `data/hdf5/`

4. **Dados de Consumo Consolidados**
   - **Fonte**: `data_consolidation.py`  
   - **Diretório**: `data/consolidated/`

5. **Resultados da Amostra**
   - **Fonte**: `sample_selection.py`  
   - **Diretórios**:
     - `data/samples/group_1/`
     - `data/samples/group_2/`
     - `data/samples/group_3/`
     - `data/samples/group_4/`
     - `data/samples/group_5/`
     - `data/samples/merged/`

---

## Procedimento

### 0. Instalar Requisitos

- **Instruções**: 
  ```bash
  pip install -r requirements.txt
  ```

### 1. Preparar Estrutura Básica de Dados

- **Instruções**:  
  Leia `data_structure.txt` e organize os dados adequadamente nos diretórios.

---

### 2. Criar Mapeamento HDF5

- **O que faz**:  
  Cria um mapeamento entre inteiros e códigos de medidores (números de série) para criação eficiente de arquivos HDF5.

- **Como executar**:  
  ```bash
  python hdf5_mappings.py
  ```

---

### 3. Limpar Arquivos CSV com Dados de Consumo Horário

- **O que faz**:  
  - Remove colunas desnecessárias: Remove as colunas `ubicacion` e `tip_lectura` do DataFrame.
  - Converte `fec_interval` para datetime: Garante que a coluna `fec_interval` esteja no formato de data e hora correto.
  - Filtra o DataFrame: Mantém apenas linhas onde `fec_interval` tem minutos e segundos iguais a 0 (ou seja, timestamps arredondados para a hora).
  - Limpa a coluna `consumo`: Converte `consumo` para valores numéricos, substituindo entradas não numéricas por 0 e definindo valores negativos como 0.
  - Cria uma cópia limpa: Salva os dados CSV limpos no diretório `data/recent_measurements_clean/`.

- **Como executar**:  
  ```bash
  python clean_csv_data.py
  ```

---

### 4\. Criar Arquivo de Medições HDF5

-   **O que faz**: Cria um arquivo HDF5 contendo os dados de medições. O arquivo final é aproximadamente 20 vezes menor que os arquivos CSV originais.

-   **Como executar**:

    **No Linux**:

    1.  Instale as bibliotecas necessárias:

    ```bash
    sudo apt-get install libhdf5-dev libhdf5-cpp-100
    sudo apt-get install g++
    ```

    2.  Compile o programa em C++:
    ```bash
    sudo apt-get install libhdf5-dev libhdf5-cpp-100
    sudo apt-get install g++
    ```

    3.  Execute o programa:
    ```bash
    ./csv_to_hdf5
    ```

    **No Windows**:

    1.  Instale **MinGW-w64** ou um compilador similar.

    2.  Baixe e instale **HDF5 Precompiled Binaries**.
        -   Extraia os arquivos baixados e anote o diretório de instalação (e.g., `C:\HDF5`).

    3.  Configure as variáveis de ambiente:
        -   Adicione `C:\HDF5\include` à variável `INCLUDE`.
        -   Adicione `C:\HDF5\lib` à variável `LIB`.
        -   Adicione `C:\HDF5\bin` ao `Path` para execução.

    4.  Compile usando MinGW-w64:    
    ```bash
    g++ -O3 -std=c++11 -o csv_to_hdf5 csv_to_hdf5.cpp -IC:/HDF5/include -LC:/HDF5/lib -lhdf5_cpp -lhdf5 -lz -lm
    ```
    5.  Execute o programa:
    ```bash
    csv_to_hdf5.exe    
    ```

---

### 5\. Criar Arquivo de Consumo HDF5

-   **O que faz**: Gera um arquivo HDF5 contendo os dados de consumo.

-   **Como executar**:

    Siga os mesmos passos de compilação e execução descritos no **Passo 4**, mas compile o arquivo `create_consumptions_hdf5.cpp`.

    **No Linux**:
    ```bash
    g++ -O3 -march=native -std=c++11 -o create_consumptions_hdf5 create_consumptions_hdf5.cpp \
    -I/usr/include/hdf5/serial -L/usr/lib/x86_64-linux-gnu/hdf5/serial \
    -lhdf5_cpp -lhdf5 -lz -ldl -lm
    ./create_consumptions_hdf5   
    ```

    **No Windows**:
    ```bash
    g++ -O3 -std=c++11 -o create_consumptions_hdf5 create_consumptions_hdf5.cpp -IC:/HDF5/include -LC:/HDF5/lib -lhdf5_cpp -lhdf5 -lz -lm
    create_consumptions_hdf5.exe 
    ```

---

### 6\. Consolidar Dados da ENEL SP

-   **O que faz**: Consolida várias fontes de dados em um único arquivo `enel_north_march_income_iicc_ene.csv`. Este arquivo contém dados de consumidores do norte da ENEL, incluindo renda média, IICC (para março), e consumo médio de energia. O script também lida com lacunas e erros nos dados.

-   **Como executar**:
    ```bash
    python data_consolidation.py
    ```

---

### 7\. Selecionar Amostras

-   **O que faz**: Seleciona amostras aleatórias para cada estrato e as salva como arquivos `.xlsx`.

-   **Como executar**:
    ```bash
    python sample_selection.py
    ```

---

### 8\. Grupos de Amostras

-   **Descrição**:

    As amostras são divididas em grupos 1 a 5. Há também uma amostra mesclada por estratos.

    -   **Grupo 1**: Grupo de controle (100 em cada estrato)
    -   **Grupo 2**: Rebate no Horário de Pico (170 em cada estrato)
    -   **Grupo 3**: Rebate no Horário de Pico + Aparelho Inteligente (170 em cada estrato)
    -   **Grupo 4**: Tarifa Trinomial por Horário de Uso (280 em cada estrato)
    -   **Grupo 5**: Tarifa Trinomial por Horário de Uso + Rebate no Horário de Pico (280 em cada estrato)
    -   **Mesclado**: 1.000 em cada estrato

---

### Seleção de Amostras no Mapa:

![Data Flow Diagram](data/samples_map/map_with_samples_selected.png)

---

# ENEL Sample Selection

### All the required files and in the Google Drive:

https://drive.google.com/drive/folders/1-BZ1XkxEed1XD6x2veJk3TdDF3Aw_Izz?usp=sharing

Files list with their respective Hash (SHA256):

1. SP_Exceto_Capital_20231030.zip
   - Hash: 0331e2e5413f5cf5812851104dda5a45e19754169e2b77120692d48a64fdc222

2. samples.zip
   - Hash: 0bbe123229a24fd6d1ca62506fa6e9af75f5fe75ed15fb7df9975a0022b1ae38

3. sp_setores_censitarios_2010.zip
   - Hash: 7c5238e122cefa26f7e8817097d637da8ab9c2c94e5f5a7e66c2222ec7e3f789

4. complementary.zip
   - Hash: f53bc06b36a321ec97afe6c7da6dd94ea6cda27a5303d6b5fc96a23b8b60c5e7

5. SP_Capital_20231030.zip
   - Hash: c5253fd5bcef2d31e206165fc337ddb91e4e49acb5c55a4c253eb728d08e2401

6. UCBT.zip
   - Hash: fd80ffc5888bec362a594bba803d502d2f83ff4bdfe00b45c031f4776edf2f71

7. recent_measurements.zip
   - Hash: 55010c936ae2580b413bf52342f1ae03b94d576f36206d4a473df22759082e55

## Data Structure

### Inputs (Required as Data Source)

1. **Hourly Measurements (January to April) of Advanced Meters**
   - **Source**: ENEL  
   - **Files**: 121 CSV files  
   - **Instructions**:  
     Extract the file `recent_measurements.zip` from Google Drive and place it in the `data/recent_measurements/` directory.  
     - Data structure:  
       `data/recent_measurements/{csv_file}`

2. **Monthly Consumption (2023) for Consumers with Advanced Meters and Atypical Lists**
   - **Source**: ENEL  
   - **Files**: CSV, XLSX  
   - **Instructions**:  
     Extract the file `complementary.zip` from Google Drive and place it in the `data/complementary/` directory.  
     - Data structure:  
       - `data/complementary/Base_SM.csv`  
       - `data/complementary/atipicos.xlsx`  
       - `data/complementary/baixa_renda.xlsx`

3. **BDGD Data (Shapefiles) for Consumption (2023)**
   - **Source**: ENEL  
   - **Files**: Shapefiles  
   - **Instructions**:  
     Extract the file `UCBT.zip` from Google Drive and place it in the `data/UCBT/` directory.  
     - Data structure:  
       `data/UCBT/{shapefile}`

4. **Census Data for SP (2010)**
   - **Source**: IBGE  
   - **Files**: ZIP files  
   - **Instructions**:  
     Extract the files `SP_Capital_20231030.zip`, `SP_Exceto_Capital_20231030.zip`, and `sp_setores_censitarios_2010.zip` from Google Drive and place them in the appropriate directories.  
     - Data structure:  
       - `data/sp_setores_censitarios_2010/{...}`  
       - `data/SP_Capital_20231030/{...}`  
       - `data/SP_Exceto_Capital_20231030/{...}`

### Outputs (Directory Structure for Intermediary and Final Outputs)

Make sure the following empty directories exist:

1. **Meters to Integers and Integers to Meters Dictionaries**
   - **Source**: `hdf5_mappings.py`  
   - **Directory**: `data/misc/`

2. **Cleaned Hourly Measurements**
   - **Source**: `clean_csv_data.py`  
   - **Directory**: `data/recent_measurements_clean/`

3. **HDF5 Data (Memory Efficient)**
   - **Directory**: `data/hdf5/`

4. **Consolidated Consumption Data**
   - **Source**: `data_consolidation.py`  
   - **Directory**: `data/consolidated/`

5. **Sample Results**
   - **Source**: `sample_selection.py`  
   - **Directories**:
     - `data/samples/group_1/`
     - `data/samples/group_2/`
     - `data/samples/group_3/`
     - `data/samples/group_4/`
     - `data/samples/group_5/`
     - `data/samples/merged/`

---

## Procedure

### 0. Install Requirements

- **Instructions**: 
  ```bash
  pip install -r requirements.txt
  ```

### 1. Prepare Basic Data Structure

- **Instructions**:  
  Read `data_structure.txt` and arrange the data accordingly in the directories.

---

### 2. Create HDF5 Mapping

- **What it does**:  
  Creates a mapping between integers and meter codes (serial numbers) for efficient HDF5 file creation.

- **How to run**:  
  ```bash
  python hdf5_mappings.py
  ```

---

### 3. Clean CSV Files with Hourly Consumption Data

- **What it does**:  
  - Removes unnecessary columns: Drops the columns `ubicacion` and `tip_lectura` from the DataFrame.
  - Converts `fec_interval` to datetime: Ensures the `fec_interval` column is in proper datetime format.
  - Filters the DataFrame: Keeps only rows where `fec_interval` has minute and second equal to 0 (i.e., round-hour timestamps).
  - Cleans the `consumo` column: Converts `consumo` to numeric values, replacing non-numeric entries with 0 and setting negative values to 0.
  - Creates a clean copy: Saves the cleaned CSV data in the directory `data/recent_measurements_clean/`.

- **How to run**:  
  ```bash
  python clean_csv_data.py
  ```

---

### 4\. Create HDF5 Measurements File

-   **What it does**: Creates an HDF5 file containing the measurements data. The final file size is approximately 20 times smaller than the original CSV files.

-   **How to run**:

    **On Linux**:

    1.  Install necessary libraries:

    ```bash
    sudo apt-get install libhdf5-dev libhdf5-cpp-100
    sudo apt-get install g++
    ```

    2.  Compile the C++ program:
    ```bash
    sudo apt-get install libhdf5-dev libhdf5-cpp-100
    sudo apt-get install g++
    ```

    3.  Run the program:
    ```bash
    ./csv_to_hdf5
    ```

    **On Windows**:

    1.  Install **MinGW-w64** or a similar compiler.

    2.  Download and install **HDF5 Precompiled Binaries**.
        -   Extract the downloaded files and note the installation directory (e.g., `C:\HDF5`).

    3.  Set up environment variables:
        -   Add `C:\HDF5\include` to the `INCLUDE` variable.
        -   Add `C:\HDF5\lib` to the `LIB` variable.
        -   Add `C:\HDF5\bin` to the `Path` variable for runtime.

    4.  Compile using MinGW-w64:    
    ```bash
    .g++ -O3 -std=c++11 -o csv_to_hdf5 csv_to_hdf5.cpp -IC:/HDF5/include -LC:/HDF5/lib -lhdf5_cpp -lhdf5 -lz -lm
    ```
    5.  Run the program:
    ```bash
    csv_to_hdf5.exe    
    ```
---

### 5\. Create HDF5 Consumption File

-   **What it does**: Generates an HDF5 file containing the consumption data.

-   **How to run**:

    Follow the same compilation and execution steps as in **Step 4**, but compile the `create_consumptions_hdf5.cpp` file instead.

    **On Linux**:
    ```bash
    g++ -O3 -march=native -std=c++11 -o create_consumptions_hdf5 create_consumptions_hdf5.cpp \
    -I/usr/include/hdf5/serial -L/usr/lib/x86_64-linux-gnu/hdf5/serial \
    -lhdf5_cpp -lhdf5 -lz -ldl -lm
    ./create_consumptions_hdf5   
    ```

    **On Windows**:
    ```bash
    g++ -O3 -std=c++11 -o create_consumptions_hdf5 create_consumptions_hdf5.cpp -IC:/HDF5/include -LC:/HDF5/lib -lhdf5_cpp -lhdf5 -lz -lm
    create_consumptions_hdf5.exe 
    ```

---

### 6\. Consolidate ENEL SP Data

-   **What it does**: Consolidates multiple data sources into a single file `enel_north_march_income_iicc_ene.csv`. This file contains data for consumers in the north of ENEL, including average income, IICC (for March), and average energy consumption. The script also handles data gaps and errors.

-   **How to run**:
    ```bash
    python data_consolidation.py
    ```

---

### 7\. Select Samples

-   **What it does**: Selects random samples for each stratum and saves them as `.xlsx` files.

-   **How to run**:
    ```bash
    python sample_selection.py
    ```

---

### 8\. Sample Groups

-   **Description**:

    Samples are split into groups 1 to 5. There is also a merged sample by strata.

    -   **Group 1**: Control group (100 in each stratum)
    -   **Group 2**: Peak Time Rebate (170 in each stratum)
    -   **Group 3**: Peak Time Rebate + Smart Appliance (170 in each stratum)
    -   **Group 4**: Trinomial Time of Use (280 in each stratum)
    -   **Group 5**: Trinomial Time of Use + Peak Time Rebate (280 in each stratum)
    -   **Merged**: 1,000 in each stratum

---

### Sample Selection on Map:

![Data Flow Diagram](data/samples_map/map_with_samples_selected.png)

