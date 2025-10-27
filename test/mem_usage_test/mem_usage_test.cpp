#include <iostream>
#include <fstream>
#include <sstream>
#include <unistd.h>
#include <sys/syscall.h>

void mem_usage(double& vm_usage, double& rss, double& swap) {
    rss = 0.0;
    swap = 0.0;

    // Read RSS from /proc/self/stat
    std::ifstream stat_stream("/proc/276842/stat", std::ios_base::in);
    if (!stat_stream.is_open()) {
        std::cerr << "Failed to open /proc/self/stat" << std::endl;
        return;
    }

    std::string pid, comm, state, ppid, pgrp, session, tty_nr;
    std::string tpgid, flags, minflt, cminflt, majflt, cmajflt;
    std::string utime, stime, cutime, cstime, priority, nice;
    std::string O, itrealvalue, starttime;
    unsigned long vsize;
    long rss_pages;

    stat_stream >> pid >> comm >> state >> ppid >> pgrp >> session >> tty_nr >>
                tpgid >> flags >> minflt >> cminflt >> majflt >> cmajflt >> utime >>
                stime >> cutime >> cstime >> priority >> nice >> O >> itrealvalue >>
                starttime >> vsize >> rss_pages;

    stat_stream.close();

    long page_size_kb = sysconf(_SC_PAGE_SIZE) / 1024;  // for x86-64 is configured to use 2MB pages   KB
    vm_usage = vsize / 1024.0; // KB
    rss = rss_pages * page_size_kb; // KB

    // Read Swap from /proc/self/status
    std::ifstream status_stream("/proc/276842/status", std::ios_base::in);
    if (!status_stream.is_open()) {
        std::cerr << "Failed to open /proc/self/status" << std::endl;
        return;
    }

    std::string line;
    while (std::getline(status_stream, line)) {
        if (line.find("VmSwap:") != std::string::npos) {
            std::istringstream iss(line);
            std::string key, value, unit;
            iss >> key >> value >> unit;
            swap = std::stod(value);
            break;
        }
    }

    status_stream.close();
}

int main() {
    std::remove("memory_usage.log");
    double vm_usage, rss, swap;
    std::ofstream output_file("memory_usage.log", std::ios_base::app);  // Open file in append mode

    if (!output_file.is_open()) {
        std::cerr << "Failed to open output file." << std::endl;
        return 1;
    }

    while (true) {
        mem_usage(vm_usage, rss, swap);
        output_file << "Virtual Memory Usage: " << (vm_usage / 1024) << " MB, " << "Resident Set Size: " << (rss / 1024) << " MB, Swap: " << swap << " MB\n";
        output_file.flush();  // Ensure data is written to the file immediately
        sleep(10);  // Sleep for 10 second
    }

    output_file.close();
    return 0;
}