#define GLOBAL_VALUE_DEFINE

#include "protobuf/types.pb.h"
#include "protobuf/remote.pb.h"
#include <snappy.h>

#include "SpanRemoteDB.h"
#include "db/DB.hpp"
#include "db/RemoteDB.h"
#include "gtest/gtest.h"

int main(int argc, char** argv) {
    std::string sep_dbpath = "/tmp/sep_db";
    std::string dbpath = "/tmp/tsdb_big";
    std::string logpath = "/tmp/tsdb_big_log";

    tsdb::db::SpanRemoteDB db(sep_dbpath, dbpath, logpath);
    sleep(1);

    std::cout<< "Successfully start remote storage service." <<std::endl;
    for(;;) {
        //        sleep(1);
    }
}