#include "atomic_queue/atomic_queue.h"

#include <thread>
#include <cstdint>
#include <iostream>

int main() {
    int thread_num = 8;
    uint capacity = 1024;
    atomic_queue::AtomicQueueB<uint32_t, std::allocator<uint32_t>, static_cast<uint32_t>(-1)> atm_queue{capacity};
    atomic_queue::AtomicQueue<uint32_t >

    std::thread writers[thread_num];
    for (int i = 0; i < thread_num; ++i) {
        writers[i] = std::thread([&atm_queue, &i]() {
           for (int j = 0; j < 100; j++) {
               atm_queue.push(i*100+j);
           }
        });
    }

    for (auto& t : writers) {
        t.join();
    }

    assert(atm_queue.was_size() == thread_num*100);
    while (!atm_queue.was_empty()) {
        auto num = atm_queue.pop();
        std::cout<<num<<std::endl;
    }
}