#include <iostream>
#include <H5Cpp.h>
#include <chrono>
#include <vector>
#include <string>
#include <unordered_map>

// to compile on Linux:
// g++ -O3 -march=native -std=c++11 -o create_consumptions_hdf5 create_consumptions_hdf5.cpp -I/usr/include/hdf5/serial -L/usr/lib/x86_64-linux-gnu/hdf5/serial -lhdf5_cpp -lhdf5 -lz -ldl -lm

int main() {
    const int ARRAY_SIZE = 2904;
    const int NUM_GROUPS = 515470;
    const int BATCH_SIZE = 10000;

    // Use unordered_map for efficient lookup
    std::unordered_map<std::string, int*> group_data_map;
    std::unordered_map<std::string, int*> consumption_data_map;

    try {
        // Start timing the entire process
        auto start_time_total = std::chrono::high_resolution_clock::now();

        // Open the HDF5 file for reading
        H5::H5File file("data/measurements.hdf5", H5F_ACC_RDONLY);

        // Create a new HDF5 file for writing
        H5::H5File new_file("data/consumption_data.hdf5", H5F_ACC_TRUNC);

        // Get the root group
        H5::Group rootGroup = file.openGroup("/");

        // Start timing
        auto start_time = std::chrono::high_resolution_clock::now();

        // Process group names in batches
        for (int batch_start = 0; batch_start < NUM_GROUPS; batch_start += BATCH_SIZE) {
            int batch_end = std::min(batch_start + BATCH_SIZE, NUM_GROUPS);

            for (int i = batch_start; i < batch_end; ++i) {
                std::string group_name = std::to_string(i);
                H5::Group group = rootGroup.openGroup(group_name);

                // Check if the group contains a dataset named "data"
                if (group.exists("data")) {
                    H5::DataSet dataset = group.openDataSet("data");
                    H5::DataSpace dataspace = dataset.getSpace();

                    // Check if the dataset is 1D and of type int32
                    if (dataspace.getSimpleExtentNdims() == 1) {
                        hsize_t dims[1];
                        dataspace.getSimpleExtentDims(dims, NULL);

                        // Ensure the dataset size does not exceed 2904
                        if (dims[0] > ARRAY_SIZE) {
                            std::cerr << "Dataset size exceeds " << ARRAY_SIZE << " for group " << group_name << std::endl;
                            continue;
                        }

                        // Allocate memory for the individual array
                        int* data = new int[ARRAY_SIZE](); // Initialize array to zero
                        int* consumption = new int[ARRAY_SIZE](); // Initialize array to zero

                        // Read the dataset
                        dataset.read(data, H5::PredType::NATIVE_INT);

                        // Calculate the hourly consumption
                        for (int j = 1; j < ARRAY_SIZE; ++j) {
                            if (data[j] != 0 && data[j-1] != 0) {
                                int diff = data[j] - data[j-1];
                                consumption[j] = (diff >= 0) ? diff : 0;
                            } else {
                                consumption[j] = 0;
                            }
                        }

                        // Store in the maps
                        group_data_map[group_name] = data;
                        consumption_data_map[group_name] = consumption;

                        // Create the new group in the new file and write the consumption dataset
                        H5::Group new_group = new_file.createGroup(group_name);
                        hsize_t dim[] = {ARRAY_SIZE};
                        H5::DataSpace dataspace_new(1, dim);
                        H5::DataSet dataset_new = new_group.createDataSet("data", H5::PredType::NATIVE_INT, dataspace_new);
                        dataset_new.write(consumption, H5::PredType::NATIVE_INT);

                        // Print time taken for every 10,000 arrays
                        if (group_data_map.size() % BATCH_SIZE == 0) {
                            auto current_time = std::chrono::high_resolution_clock::now();
                            auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(current_time - start_time);
                            std::cout << "Time taken to add " << group_data_map.size() << " arrays: " << duration.count() << " milliseconds" << std::endl;
                            start_time = current_time; // Reset start time
                        }

                        // Check if the number of groups exceeds the maximum
                        if (group_data_map.size() >= NUM_GROUPS) {
                            std::cerr << "Number of groups exceeds the maximum allowed: " << NUM_GROUPS << std::endl;
                            break;
                        }
                    }
                }
            }

            // Check if the number of groups exceeds the maximum
            if (group_data_map.size() >= NUM_GROUPS) {
                break;
            }
        }

        // Print the first 10 items of each dataset for the first 10 groups
        int count = 0;
        for (const auto& pair : group_data_map) {
            if (count >= 10) break;
            std::cout << "Group: " << pair.first << ", First 10 items: ";
            for (int j = 0; j < 10; ++j) {
                std::cout << pair.second[j] << " ";
            }
            std::cout << std::endl;
            ++count;
        }

        // Clean up allocated memory
        for (auto& pair : group_data_map) {
            delete[] pair.second;
        }
        for (auto& pair : consumption_data_map) {
            delete[] pair.second;
        }

        // Stop timing the entire process
        auto end_time_total = std::chrono::high_resolution_clock::now();
        auto duration_total = std::chrono::duration_cast<std::chrono::milliseconds>(end_time_total - start_time_total);
        std::cout << "Total time taken: " << duration_total.count() << " milliseconds" << std::endl;

    } catch (H5::FileIException& error) {
        error.printErrorStack();
        return -1;
    } catch (H5::GroupIException& error) {
        error.printErrorStack();
        return -1;
    } catch (H5::DataSetIException& error) {
        error.printErrorStack();
        return -1;
    } catch (H5::DataSpaceIException& error) {
        error.printErrorStack();
        return -1;
    } catch (H5::DataTypeIException& error) {
        error.printErrorStack();
        return -1;
    }

    return 0;
}
