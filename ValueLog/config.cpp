#include "config.h"

namespace config {
    int tree_series_thread_pool_size = 32;
    int max_slab_memory = 8;
    int read_buffer_size = 256;
    int max_series_num = 16;     // must be defined due to its use in struct SlabInfo, so it has no usage for now
    int slab_item_size = 256;
    int chunk_size = 248;
    int slab_size = 4;
}