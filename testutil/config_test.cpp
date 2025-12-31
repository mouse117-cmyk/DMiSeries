#define GLOBAL_VALUE_DEFINE

#include "config.h"

int main() {
    std::string config_file_path = "../config.yaml";
    tsdb::testutil::ResolveConfigFile(config_file_path);
}