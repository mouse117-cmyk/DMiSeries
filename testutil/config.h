#include <yaml-cpp/yaml.h>
#include <iostream>

namespace tsdb::testutil{
    void ResolveConfigFile(std::string file) {
        YAML::Node config = YAML::LoadFile(file);

        int thread_num = config["thread_num"].as<int>();
        std::string db_path = config["db_path"].as<std::string>();
        bool use_wal = config["use_wal"].as<bool>();

        std::cout<<"thread_num: "<<thread_num<<std::endl;
        std::cout<<"db_path: "<<db_path<<std::endl;
        std::cout<<"use_wal: "<<use_wal<<std::endl;

    }
}