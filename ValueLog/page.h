

#ifndef SPANSERIES_PAGE_H
#include <stdint.h>
#include <atomic>
#define SPANSERIES_PAGE_H

namespace span {

struct PageInfo {
  uint32_t page_id_;
  std::atomic<uint32_t> offset_;
};

}

#endif  // SPANSERIES_PAGE_H
