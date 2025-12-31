#define GLOBAL_VALUE_DEFINE

#include <atomic>
#include "iostat.h"

namespace tsdb::testutil {

}

int main() {
    std::string outputFilePath = "/mnt/nvme/iostat_output.txt";

    std::atomic<bool> running(true);
    // Step 1: Collect iostat data and save to file
    tsdb::testutil::StartIostat(outputFilePath, running);
    sleep(10);
    tsdb::testutil::StopIostat(running);

    // Step 2: Parse the iostat output file
    std::vector<std::vector<std::string>> data = tsdb::testutil::ParseIostatFile(outputFilePath);

    // Step 3: Calculate average of specific columns (e.g., %util)
    double avgUtil = tsdb::testutil::CalculateAverage(data, 14); // Assuming %util is in column 14

    // Step 4: Print the results
    std::cout << "Average %util: " << avgUtil << std::endl;

    return 0;
}