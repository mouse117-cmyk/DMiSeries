#ifndef TESTUTIL_IOSTAT_H
#define TESTUTIL_IOSTAT_H

#include <iostream>
#include <fstream>
#include <string>
#include <sstream>
#include <vector>
#include <algorithm>
#include <cstdlib>
#include <ctime>
#include <thread>

namespace tsdb { namespace testutil {



// Function to execute a system command and save output to a file
    void CollectIostatData(const std::string& outputFilePath, std::atomic<bool>& running) {
        std::string command = "iostat -x 1 >> " + outputFilePath;
        while (running) {
            int result = std::system(command.c_str());
            if (result != 0) {
                std::cerr << "Failed to execute iostat command." << std::endl;
                break;
            }
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
    }

// Function to read and parse the iostat output file
    std::vector<std::vector<std::string>> ParseIostatFile(const std::string& filePath) {
        std::vector<std::vector<std::string>> data;
        std::ifstream file(filePath);
        std::string line;

        while (std::getline(file, line)) {
            std::vector<std::string> row;
            std::string token;
            std::istringstream lineStream(line);

            while (std::getline(lineStream, token, ' ')) {
                if (!token.empty()) {
                    row.push_back(token);
                }
            }

            if (!row.empty()) {
                data.push_back(row);
            }
        }

        return data;
    }

// Function to calculate average of a specific column
    double CalculateAverage(const std::vector<std::vector<std::string>>& data, int columnIndex) {
        double sum = 0.0;
        int count = 0;

        for (const auto& row : data) {
            if (row.size() > columnIndex) {
                try {
                    double value = std::stod(row[columnIndex]);
                    sum += value;
                    ++count;
                } catch (const std::invalid_argument& e) {
                    // Skip invalid entries
                }
            }
        }

        return count > 0 ? sum / count : 0.0;
    }

    std::thread StartIostat(const std::string& outputFilePath, std::atomic<bool>& running) {
        boost::filesystem::remove(outputFilePath);
        std::thread collect([outputFilePath, &running] {
            tsdb::testutil::CollectIostatData(outputFilePath, running);
        });
        return collect;
    }

    void StopIostat(std::thread& collect, std::atomic<bool>& running) {
        if (collect.joinable()) {
            running = false;
            collect.join();
        }
    }
} }


#endif
