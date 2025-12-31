#include "slab.h"
#include "config.h"

namespace slab{

    size_t FC_SLAB_MEMORY = 8;
    size_t READ_BUFFER_SIZE = 256;
    int SLAB_ITEM_SIZE = 256;
    int CHUNK_SIZE = 248;
    int SLAB_SIZE = 4;

    void Setting::SetDefault() {
        FC_SLAB_MEMORY = size_t(config::max_slab_memory) * GB;
        READ_BUFFER_SIZE = size_t(config::read_buffer_size) * MB;
        SLAB_ITEM_SIZE = config::slab_item_size;
        CHUNK_SIZE = config::chunk_size;
        SLAB_SIZE = config::slab_size * KB;

        max_slab_memory_ = FC_SLAB_MEMORY;
        ssd_device_ = "/tmp/ssd";
        ssd_slab_info_ = "/tmp/ssd_slab_info";
        port = 11211;
        addr = "";
        slab_size_ = SLAB_SIZE;
        write_batch_size_ = 1600;
        migrate_batch_size_ = 6400;
    }
}
