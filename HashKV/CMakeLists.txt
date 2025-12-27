cmake_minimum_required(VERSION 3.23)
project(hashkv)

set(CMAKE_CXX_STANDARD 17)

add_compile_options(-w)

include_directories(
        src
        src/util
        src/ds
)

find_package(Boost REQUIRED COMPONENTS thread system)

set(LIB_DIR_LEVELDB "${CMAKE_CURRENT_SOURCE_DIR}/lib/leveldb/out-shared")
set(LIB_DIR_HDR "${CMAKE_CURRENT_SOURCE_DIR}/lib/HdrHistogram_c-0.9.4/src")
set(LIB_DIR_THREADPOOL "${CMAKE_CURRENT_SOURCE_DIR}/lib/threadpool/threadpool")

set(INC_DIR_LEVELDB "${CMAKE_CURRENT_SOURCE_DIR}/lib/leveldb/include")
set(INC_DIR_HDR "${CMAKE_CURRENT_SOURCE_DIR}/lib/HdrHistogram_c-0.9.4/src")
set(INC_DIR_THREADPOOL "${CMAKE_CURRENT_SOURCE_DIR}/lib/threadpool")

include_directories(${INC_DIR_LEVELDB} ${INC_DIR_HDR} ${INC_DIR_THREADPOOL})
link_directories(${LIB_DIR_LEVELDB} ${LIB_DIR_HDR} ${LIB_DIR_THREADPOOL})

set(HASHKV_SOURCES
        src/util/debug.cc
        src/configManager.cc
        src/ds/bitmap.cc
        src/deviceManager.cc
        src/statsRecorder.cc
        src/logManager.cc
        src/ds/lru.cc
        src/ds/segmentPool.cc
        src/gcManager.cc
        src/kvServer.cc
        src/leveldbKeyManager.cc
        src/segmentGroupManager.cc
        src/valueManager.cc
)

add_library(hashkv STATIC ${HASHKV_SOURCES})

target_include_directories(hashkv PUBLIC
        src
        src/util
        src/ds
        ${INC_DIR_LEVELDB}
        ${INC_DIR_HDR}
        ${INC_DIR_THREADPOOL}
)
target_link_libraries(hashkv PRIVATE
        pthread
        ${CMAKE_CURRENT_SOURCE_DIR}/lib/leveldb/out-shared/libleveldb.so
        ${CMAKE_CURRENT_SOURCE_DIR}/lib/HdrHistogram_c-0.9.4/src/libhdr_histogram_static.a
)