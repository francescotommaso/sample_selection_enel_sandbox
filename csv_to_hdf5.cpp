#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <unordered_map>
#include <ctime>
#include <iomanip>
#include "H5Cpp.h"
#include <dirent.h>
#include <cstring>


// to compile:
// g++ -o csv_to_hdf5 csv_to_hdf5.cpp -I/usr/include/hdf5/serial/ -L/usr/lib/x86_64-linux-gnu/hdf5/serial -lhdf5_cpp -lhdf5 -std=c++11

const std::string INPUT_DIR = "data/recent_measurements_clean";
const std::string OUTPUT_FILE = "data/hdf5/output.hdf5";
const int ARRAY_SIZE = 121 * 24; // 121 days of hourly data

int calculateIndex(const std::string& timestamp) {
    std::tm t = {};
    std::istringstream ss(timestamp);
    ss >> std::get_time(&t, "%Y-%m-%d %H:%M:%S");
    if (ss.fail()) {
        std::cerr << "Failed to parse timestamp: " << timestamp << std::endl;
        return -1;
    }
    std::time_t time_temp = std::mktime(&t);
    std::tm start_tm = {0,0,0,1,0,124}; // January 1, 2024
    std::time_t start_time = std::mktime(&start_tm);
    return std::difftime(time_temp, start_time) / 3600;
}

int safeStoi(const std::string& str, const std::string& filename, int lineNo) {
    try {
        return std::stoi(str);
    } catch (const std::invalid_argument& e) {
        std::cerr << "Invalid argument for stoi conversion in file " << filename << " at line " << lineNo << ": " << str << std::endl;
        throw;
    }
}

int main() {
    std::unordered_map<int, std::vector<int>> dataMap;
    DIR *dir;
    struct dirent *ent;
    if ((dir = opendir(INPUT_DIR.c_str())) != NULL) {
        while ((ent = readdir(dir)) != NULL) {
            if (ent->d_type == DT_REG) { // Check if it's a regular file
                std::string filePath = INPUT_DIR + "/" + ent->d_name;
                std::ifstream file(filePath);
                std::string line;
                int lineNo = 0;

                std::cout << "Opening CSV file: " << filePath << std::endl;
                bool firstLine = true;
                
                while (std::getline(file, line)) {
                    lineNo++;
                    if (firstLine) {
                        firstLine = false;
                        continue; // Skip the header row
                    }
                    std::stringstream lineStream(line);
                    std::string cell;
                    std::vector<std::string> tokens;

                    while (std::getline(lineStream, cell, ',')) {
                        tokens.push_back(cell);
                    }

                    if (tokens.size() < 3) {
                        std::cerr << "Skipping incomplete line " << lineNo << " in file " << filePath << std::endl;
                        continue;
                    }

                    int meter_id = safeStoi(tokens[0], filePath, lineNo);
                    int consumo = safeStoi(tokens[1], filePath, lineNo);
                    int index = calculateIndex(tokens[2]);

                    if (dataMap.find(meter_id) == dataMap.end()) {
                        dataMap[meter_id] = std::vector<int>(ARRAY_SIZE, 0);
                    }

                    dataMap[meter_id][index] = consumo;
                }

                file.close();
                std::cout << "Closed CSV file: " << filePath << std::endl;
            }
        }
        closedir(dir);
    } else {
        std::cerr << "Could not open directory" << std::endl;
        return EXIT_FAILURE;
    }

    // Save data to HDF5
    H5::H5File file(OUTPUT_FILE, H5F_ACC_TRUNC);
    for (auto& pair : dataMap) {
        H5::Group group = file.createGroup("/" + std::to_string(pair.first));
        H5::DataSpace dataspace(H5S_SIMPLE);
        hsize_t dims[1] = {ARRAY_SIZE};
        dataspace.setExtentSimple(1, dims);
        H5::DataSet dataset = group.createDataSet("data", H5::PredType::NATIVE_INT, dataspace);
        dataset.write(pair.second.data(), H5::PredType::NATIVE_INT);
    }

    file.close();
    std::cout << "Data saved to HDF5." << std::endl;

    return 0;
}