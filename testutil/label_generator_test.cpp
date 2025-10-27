#define GLOBAL_VALUE_DEFINE
#include "label_generator.h"

namespace tsdb::testutil {

}

int main() {
    uint64_t num_ts = 100;
    std::vector<tsdb::label::Labels> lsets;
    lsets.reserve(num_ts);
    tsdb::testutil::get_devops_labels(num_ts, &lsets);

    for (auto& it : lsets) {
        std::cout<<tsdb::label::lbs_string(it)<<std::endl;
    }
    std::cout<<"lsets size: "<<lsets.size()<<std::endl;
}