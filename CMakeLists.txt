cmake_minimum_required(VERSION 3.9)

project(TempusCStore)

# set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3 -g -w -Wall -std=c++11 -lprofiler -ltcmalloc")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3 -g -w -Wall -std=c++17 -lprofiler -ljemalloc")
 # -fsanitize=undefined,address
message(STATUS "CMAKE_CXX_FLAGS ${CMAKE_CXX_FLAGS}")



set(LEVELDB_PLATFORM_NAME LEVELDB_PLATFORM_POSIX)

option(LEVELDB_BUILD_TESTS "Build LevelDB's unit tests" ON)
option(LEVELDB_BUILD_BENCHMARKS "Build LevelDB's benchmarks" ON)
option(LEVELDB_INSTALL "Install LevelDB's header and library" ON)
include_directories()
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/third_party)
include(CheckIncludeFile)
check_include_file("unistd.h" HAVE_UNISTD_H)

include(CheckLibraryExists)
check_library_exists(crc32c crc32c_value "" HAVE_CRC32C)
check_library_exists(snappy snappy_compress "" HAVE_SNAPPY)

FIND_PACKAGE(Boost COMPONENTS system filesystem iostreams unit_test_framework)
if (NOT Boost_FOUND)
    message(FATAL_ERROR "Fatal error: Boost (version >= 1.55) required.")
else()
    message(STATUS "Setting up BOOST")
    message(STATUS " Includes - ${Boost_INCLUDE_DIRS}")
    message(STATUS " Library  - ${Boost_LIBRARY_DIRS}")
    message(STATUS " boost_system ${Boost_SYSTEM_LIBRARY}")
    message(STATUS " boost_filesystem ${Boost_FILESYSTEM_LIBRARY}")
    message(STATUS " boost_iostreams ${Boost_IOSTREAMS_LIBRARY}")
    message(STATUS " boost_unit_test_framework ${Boost_UNIT_TEST_FRAMEWORK_LIBRARY}")
    include_directories(${Boost_INCLUDE_DIRS})
    link_directories(${Boost_LIBRARY_DIRS})
endif (NOT Boost_FOUND)

#set(Protobuf_PROTOC_EXECUTABLE "/usr/local/bin/protoc")
find_package(Protobuf REQUIRED)
if(PROTOBUF_FOUND)
    message(STATUS "protobuf library found")
else()
    message(FATAL_ERROR "protobuf library is needed but cant be found")
endif()
include_directories(${PROTOBUF_INCLUDE_DIRS})


find_package(Threads REQUIRED)

option(BUILD_SHARED_LIBS "Build shared libraries" ON)

# find_library(TCMALLOC_LIB tcmalloc)
# message(STATUS "TCMALLOC: ${TCMALLOC_LIB}")

set(LEVELDB_PUBLIC_INCLUDE_DIR "leveldb/include/leveldb")
set(LEVELDB_PORT_CONFIG_DIR "leveldb/include/port")

configure_file(
  "${CMAKE_SOURCE_DIR}/leveldb/port/port_config.h.in"
  "${PROJECT_BINARY_DIR}/${LEVELDB_PORT_CONFIG_DIR}/port_config.h"
)

include_directories(${GTEST_INCLUDE_DIRS})
include_directories(..)

include_directories(
  "${PROJECT_BINARY_DIR}/leveldb"
  "${PROJECT_BINARY_DIR}/leveldb/include"
  "."
  "${CMAKE_SOURCE_DIR}"
  "${CMAKE_SOURCE_DIR}/leveldb"
  "${CMAKE_SOURCE_DIR}/leveldb/include"
  "${CMAKE_SOURCE_DIR}/db"
  "${CMAKE_SOURCE_DIR}/protobuf"
  "${CMAKE_SOURCE_DIR}/testutil"
  #"~/.local/include"
)

set(PROTO_SRCS "${CMAKE_SOURCE_DIR}/db/DB.pb.cc")
set(PROTO_HDRS "${CMAKE_SOURCE_DIR}/db/DB.pb.h")

execute_process(COMMAND protoc --cpp_out=${CMAKE_SOURCE_DIR}/db/ ${CMAKE_SOURCE_DIR}/db/DB.proto -I ${CMAKE_SOURCE_DIR}/db/)

set(SOURCES
    "${PROJECT_BINARY_DIR}/${LEVELDB_PORT_CONFIG_DIR}/port_config.h"
    "${CMAKE_SOURCE_DIR}/leveldb/db/builder.cc"
    "${CMAKE_SOURCE_DIR}/leveldb/db/builder.h"
    "${CMAKE_SOURCE_DIR}/leveldb/db/c.cc"
    "${CMAKE_SOURCE_DIR}/leveldb/db/db_impl.cc"
    "${CMAKE_SOURCE_DIR}/leveldb/db/db_impl.h"
    "${CMAKE_SOURCE_DIR}/leveldb/db/db_iter.cc"
    "${CMAKE_SOURCE_DIR}/leveldb/db/db_iter.h"
    "${CMAKE_SOURCE_DIR}/leveldb/db/dbformat.cc"
    "${CMAKE_SOURCE_DIR}/leveldb/db/dbformat.h"
    "${CMAKE_SOURCE_DIR}/leveldb/db/dumpfile.cc"
    "${CMAKE_SOURCE_DIR}/leveldb/db/filename.cc"
    "${CMAKE_SOURCE_DIR}/leveldb/db/filename.h"
    "${CMAKE_SOURCE_DIR}/leveldb/db/file_util.cc"
    "${CMAKE_SOURCE_DIR}/leveldb/db/file_util.h"
    "${CMAKE_SOURCE_DIR}/leveldb/db/log_format.h"
    "${CMAKE_SOURCE_DIR}/leveldb/db/log_reader.cc"
    "${CMAKE_SOURCE_DIR}/leveldb/db/log_reader.h"
    "${CMAKE_SOURCE_DIR}/leveldb/db/log_writer.cc"
    "${CMAKE_SOURCE_DIR}/leveldb/db/log_writer.h"
    "${CMAKE_SOURCE_DIR}/leveldb/db/memtable.cc"
    "${CMAKE_SOURCE_DIR}/leveldb/db/memtable.h"
    "${CMAKE_SOURCE_DIR}/leveldb/db/repair.cc"
    "${CMAKE_SOURCE_DIR}/leveldb/db/skiplist.h"
    "${CMAKE_SOURCE_DIR}/leveldb/db/snapshot.h"
    "${CMAKE_SOURCE_DIR}/leveldb/db/table_cache.cc"
    "${CMAKE_SOURCE_DIR}/leveldb/db/table_cache.h"
    "${CMAKE_SOURCE_DIR}/leveldb/db/version_edit.cc"
    "${CMAKE_SOURCE_DIR}/leveldb/db/version_edit.h"
    "${CMAKE_SOURCE_DIR}/leveldb/db/version_set.cc"
    "${CMAKE_SOURCE_DIR}/leveldb/db/version_set.h"
    "${CMAKE_SOURCE_DIR}/leveldb/db/write_batch_internal.h"
    "${CMAKE_SOURCE_DIR}/leveldb/db/write_batch.cc"
    "${CMAKE_SOURCE_DIR}/leveldb/port/port_stdcxx.h"
    "${CMAKE_SOURCE_DIR}/leveldb/port/port_stdcxx.cc"
    "${CMAKE_SOURCE_DIR}/leveldb/port/port.h"
    "${CMAKE_SOURCE_DIR}/leveldb/port/thread_annotations.h"
    "${CMAKE_SOURCE_DIR}/leveldb/table/block_builder.cc"
    "${CMAKE_SOURCE_DIR}/leveldb/table/block_builder.h"
    "${CMAKE_SOURCE_DIR}/leveldb/table/block.cc"
    "${CMAKE_SOURCE_DIR}/leveldb/table/block.h"
    "${CMAKE_SOURCE_DIR}/leveldb/table/filter_block.cc"
    "${CMAKE_SOURCE_DIR}/leveldb/table/filter_block.h"
    "${CMAKE_SOURCE_DIR}/leveldb/table/format.cc"
    "${CMAKE_SOURCE_DIR}/leveldb/table/format.h"
    "${CMAKE_SOURCE_DIR}/leveldb/table/iterator_wrapper.h"
    "${CMAKE_SOURCE_DIR}/leveldb/table/iterator.cc"
    "${CMAKE_SOURCE_DIR}/leveldb/table/merger.cc"
    "${CMAKE_SOURCE_DIR}/leveldb/table/merger.h"
    "${CMAKE_SOURCE_DIR}/leveldb/table/table_builder.cc"
    "${CMAKE_SOURCE_DIR}/leveldb/table/table.cc"
    "${CMAKE_SOURCE_DIR}/leveldb/table/two_level_iterator.cc"
    "${CMAKE_SOURCE_DIR}/leveldb/table/two_level_iterator.h"
    "${CMAKE_SOURCE_DIR}/leveldb/util/arena.cc"
    "${CMAKE_SOURCE_DIR}/leveldb/util/arena.h"
    "${CMAKE_SOURCE_DIR}/leveldb/util/bloom.cc"
    "${CMAKE_SOURCE_DIR}/leveldb/util/cache.cc"
    "${CMAKE_SOURCE_DIR}/leveldb/util/coding.cc"
    "${CMAKE_SOURCE_DIR}/leveldb/util/coding.h"
    "${CMAKE_SOURCE_DIR}/leveldb/util/comparator.cc"
    "${CMAKE_SOURCE_DIR}/leveldb/util/crc32c.cc"
    "${CMAKE_SOURCE_DIR}/leveldb/util/crc32c.h"
    "${CMAKE_SOURCE_DIR}/leveldb/util/env.cc"
    "${CMAKE_SOURCE_DIR}/leveldb/util/filter_policy.cc"
    "${CMAKE_SOURCE_DIR}/leveldb/util/hash.cc"
    "${CMAKE_SOURCE_DIR}/leveldb/util/hash.h"
    "${CMAKE_SOURCE_DIR}/leveldb/util/io_posix.cc"
    "${CMAKE_SOURCE_DIR}/leveldb/util/io_posix.h"
    "${CMAKE_SOURCE_DIR}/leveldb/util/logging.cc"
    "${CMAKE_SOURCE_DIR}/leveldb/util/logging.h"
    "${CMAKE_SOURCE_DIR}/leveldb/util/mutexlock.h"
    "${CMAKE_SOURCE_DIR}/leveldb/util/no_destructor.h"
    "${CMAKE_SOURCE_DIR}/leveldb/util/options.cc"
    "${CMAKE_SOURCE_DIR}/leveldb/util/random.h"
    "${CMAKE_SOURCE_DIR}/leveldb/util/status.cc"
    "${CMAKE_SOURCE_DIR}/leveldb/util/string_util.cc"
    "${CMAKE_SOURCE_DIR}/leveldb/util/string_util.h"
    "${CMAKE_SOURCE_DIR}/leveldb/util/xxhash.cc"
    "${CMAKE_SOURCE_DIR}/leveldb/util/xxhash.h"

    "${CMAKE_SOURCE_DIR}/base/Checksum.cpp"
    "${CMAKE_SOURCE_DIR}/base/Endian.cpp"
    "${CMAKE_SOURCE_DIR}/base/Logging.cpp"
    "${CMAKE_SOURCE_DIR}/base/LogStream.cpp"
    "${CMAKE_SOURCE_DIR}/base/Rune.cpp"
    "${CMAKE_SOURCE_DIR}/block/Block.cpp"
    "${CMAKE_SOURCE_DIR}/block/BlockUtils.cpp"
    "${CMAKE_SOURCE_DIR}/chunk/BitStream.cpp"
    "${CMAKE_SOURCE_DIR}/chunk/ChunkReader.cpp"
    "${CMAKE_SOURCE_DIR}/chunk/ChunkUtils.cpp"
    "${CMAKE_SOURCE_DIR}/chunk/ChunkWriter.cpp"
    "${CMAKE_SOURCE_DIR}/chunk/DeleteIterator.cpp"
    "${CMAKE_SOURCE_DIR}/chunk/XORAppender.cpp"
    "${CMAKE_SOURCE_DIR}/chunk/XORChunk.cpp"
    "${CMAKE_SOURCE_DIR}/chunk/XORIterator.cpp"
    "${CMAKE_SOURCE_DIR}/compact/CompactionChunkSeriesSet.cpp"
    "${CMAKE_SOURCE_DIR}/compact/LeveledCompactor.cpp"
    "${CMAKE_SOURCE_DIR}/compact/MergedChunkSeriesSet.cpp"
    "${CMAKE_SOURCE_DIR}/db/DB.cpp"
    "${CMAKE_SOURCE_DIR}/db/DBUtils.cpp"
    "${CMAKE_SOURCE_DIR}/db/HeapUtils.h"
    "${CMAKE_SOURCE_DIR}/db/JSONUtils.h"
    "${CMAKE_SOURCE_DIR}/disk/log_manager.cc"
    "${CMAKE_SOURCE_DIR}/db/HttpParser.cpp"
    "${CMAKE_SOURCE_DIR}/head/Head.cpp"
    "${CMAKE_SOURCE_DIR}/head/TreeHead.cpp"
    "${CMAKE_SOURCE_DIR}/head/SpanHead.cpp"
    "${CMAKE_SOURCE_DIR}/head/HeadSeriesSet.cpp"
    "${CMAKE_SOURCE_DIR}/head/HeadUtils.cpp"
    "${CMAKE_SOURCE_DIR}/head/MemSeries.cpp"
    "${CMAKE_SOURCE_DIR}/head/TreeMemSeries.cpp"
    "${CMAKE_SOURCE_DIR}/head/SeriesHashmap.cpp"
    "${CMAKE_SOURCE_DIR}/head/StripeSeries.cpp"
    "${CMAKE_SOURCE_DIR}/head/LFStripeSeries.cpp"
    "${CMAKE_SOURCE_DIR}/index/IndexReader.cpp"
    "${CMAKE_SOURCE_DIR}/index/IndexUtils.cpp"
    "${CMAKE_SOURCE_DIR}/index/IndexWriter.cpp"
    "${CMAKE_SOURCE_DIR}/index/IntersectPostings.cpp"
    "${CMAKE_SOURCE_DIR}/index/ListPostings.cpp"
    "${CMAKE_SOURCE_DIR}/index/MemPostings.cpp"
    "${CMAKE_SOURCE_DIR}/index/SubtractionPostings.cpp"
    "${CMAKE_SOURCE_DIR}/index/ThmapPostings.cpp"
    "${CMAKE_SOURCE_DIR}/index/MergeSetPostings.cpp"
    "${CMAKE_SOURCE_DIR}/index/TOC.cpp"
    "${CMAKE_SOURCE_DIR}/index/Uint32BEPostings.cpp"
    "${CMAKE_SOURCE_DIR}/index/UnionPostings.cpp"
    "${CMAKE_SOURCE_DIR}/index/UvarintPostings.cpp"
    "${CMAKE_SOURCE_DIR}/label/Label.cpp"
    "${CMAKE_SOURCE_DIR}/mem/go_art.cc"
    "${CMAKE_SOURCE_DIR}/mem/go_art_gc.cc"
    "${CMAKE_SOURCE_DIR}/mem/go_art_serialization.cc"
    "${CMAKE_SOURCE_DIR}/mem/indirection_manager.cc"
    "${CMAKE_SOURCE_DIR}/mem/indirection_mapping.cpp"
    "${CMAKE_SOURCE_DIR}/mem/indirection_mapping_management.cpp"
    "${CMAKE_SOURCE_DIR}/mem/inverted_index.cc"
    "${CMAKE_SOURCE_DIR}/mem/mem_postings.cc"
    "${CMAKE_SOURCE_DIR}/mem/mem_postings_seda.cc"
    "${CMAKE_SOURCE_DIR}/mem/mergeset.cc"
    "${CMAKE_SOURCE_DIR}/mem/mergeset_builder.cc"
    "${CMAKE_SOURCE_DIR}/mem/mergeset_iter.cc"
    "${CMAKE_SOURCE_DIR}/mem/mergeset_manager.cc"
    "${CMAKE_SOURCE_DIR}/mem/mergeset_version_edit.cc"
    "${CMAKE_SOURCE_DIR}/mem/mergeset_version_set.cc"
    "${CMAKE_SOURCE_DIR}/mem/mmap.cc"
    "${CMAKE_SOURCE_DIR}/querier/tsdb_querier.cc"
    "${CMAKE_SOURCE_DIR}/querier/QuerierUtils.cpp"
    "${CMAKE_SOURCE_DIR}/third_party/art.cc"
    "${CMAKE_SOURCE_DIR}/third_party/art_optlock.cc"
    "${CMAKE_SOURCE_DIR}/third_party/art_optlock_epoch.cc"
    "${CMAKE_SOURCE_DIR}/third_party/ulid_uint128.cpp"
    "${CMAKE_SOURCE_DIR}/third_party/xxhash.cpp"
    "${CMAKE_SOURCE_DIR}/third_party/persistent_cedarpp.h"
    "${CMAKE_SOURCE_DIR}/tombstone/Interval.cpp"
    "${CMAKE_SOURCE_DIR}/tombstone/TombstoneUtils.cpp"
    "${CMAKE_SOURCE_DIR}/tsdbutil/RecordDecoder.cpp"
    "${CMAKE_SOURCE_DIR}/tsdbutil/RecordEncoder.cpp"
    "${CMAKE_SOURCE_DIR}/tsdbutil/tsdbutils.cpp"
    "${CMAKE_SOURCE_DIR}/wal/checkpoint.cpp"
    "${CMAKE_SOURCE_DIR}/wal/WAL.cpp"
    "${CMAKE_SOURCE_DIR}/wal/WALUtils.cpp"
    "${CMAKE_SOURCE_DIR}/parallel_wal/free_lock_wal.cpp"
    "${CMAKE_SOURCE_DIR}/parallel_wal/parallel_wal.cpp"
    "${CMAKE_SOURCE_DIR}/parallel_wal/parallel_wal_span.cpp"
  #   "${CMAKE_SOURCE_DIR}/gloal_index/metric_table.h"

#   ************************************ slab management ***************************************
    "${CMAKE_SOURCE_DIR}/TreeSeries/slab.h"
    "${CMAKE_SOURCE_DIR}/TreeSeries/slab.cpp"
    "${CMAKE_SOURCE_DIR}/TreeSeries/config.h"
    "${CMAKE_SOURCE_DIR}/TreeSeries/config.cpp"
    "${CMAKE_SOURCE_DIR}/TreeSeries/slab_management.h"
#    "${CMAKE_SOURCE_DIR}/TreeSeries/ThreadPool.h"
#    "${CMAKE_SOURCE_DIR}/TreeSeries/setting.h"
    "${CMAKE_SOURCE_DIR}/TreeSeries/slab_management.cpp"
    "${CMAKE_SOURCE_DIR}/TreeSeries/TreeSeries.h"
    "${CMAKE_SOURCE_DIR}/TreeSeries/TreeSeries.cpp"
    "${CMAKE_SOURCE_DIR}/protobuf/types.pb.h"
    "${CMAKE_SOURCE_DIR}/protobuf/remote.pb.h"
    "${CMAKE_SOURCE_DIR}/protobuf/types.pb.cc"
    "${CMAKE_SOURCE_DIR}/protobuf/remote.pb.cc"
    "${CMAKE_SOURCE_DIR}/db/RemoteDB.h"
    "${CMAKE_SOURCE_DIR}/db/RemoteDB.cpp"
    "${CMAKE_SOURCE_DIR}/db/TreeRemoteDB.h"
    "${CMAKE_SOURCE_DIR}/db/TreeRemoteDB.cpp"
    "${CMAKE_SOURCE_DIR}/db/SpanRemoteDB.h"
    "${CMAKE_SOURCE_DIR}/db/SpanRemoteDB.cpp"
    "${CMAKE_SOURCE_DIR}/db/hpg_algorithm.h"
    "${CMAKE_SOURCE_DIR}/db/hpg_algorithm.cpp"
    "${CMAKE_SOURCE_DIR}/testutil/label_generator.h"
    "${CMAKE_SOURCE_DIR}/testutil/iostat.h"
    "${CMAKE_SOURCE_DIR}/testutil/memory_usage.h"
    "${CMAKE_SOURCE_DIR}/testutil/config.h"

    "${CMAKE_SOURCE_DIR}/querier/tsdb_tree_querier.h"
    "${CMAKE_SOURCE_DIR}/querier/tsdb_tree_querier.cpp"

    "${CMAKE_SOURCE_DIR}/querier/SpanQuerier.h"
    "${CMAKE_SOURCE_DIR}/querier/SpanQuerier.cpp"

        ${PROTO_SRCS}
    ${PROTO_HDRS}
)

add_library(TSDBLIB ${SOURCES})

target_sources(TSDBLIB
  PRIVATE
    "${CMAKE_SOURCE_DIR}/leveldb/util/env_posix.cc"
    "${CMAKE_SOURCE_DIR}/leveldb/util/posix_logger.h"
)

target_sources(TSDBLIB
  PRIVATE
    "${CMAKE_SOURCE_DIR}/leveldb/helpers/memenv/memenv.cc"
    "${CMAKE_SOURCE_DIR}/leveldb/helpers/memenv/memenv.h"
)

target_compile_definitions(TSDBLIB
  PRIVATE
    # Used by include/export.h when building shared libraries.
    LEVELDB_COMPILE_LIBRARY
    # Used by port/port.h.
    ${LEVELDB_PLATFORM_NAME}=1
)

if(HAVE_CRC32C)
  target_link_libraries(TSDBLIB crc32c)
endif(HAVE_CRC32C)
# if(HAVE_SNAPPY)
  target_link_libraries(TSDBLIB snappy)
# endif(HAVE_SNAPPY)
# if(HAVE_TCMALLOC)
#   target_link_libraries(TSDBLIB tcmalloc)
# endif(HAVE_TCMALLOC)
find_package(Threads REQUIRED)
target_link_libraries(TSDBLIB Threads::Threads)

target_link_libraries(TSDBLIB ${CMAKE_SOURCE_DIR}/third_party/thmap/murmurhash.o ${CMAKE_SOURCE_DIR}/third_party/thmap/thmap.o)

target_link_libraries(TSDBLIB
  ${Boost_IOSTREAMS_LIBRARY}
  ${Boost_FILESYSTEM_LIBRARY}
  ${Boost_SYSTEM_LIBRARY}
  ${GTEST_LIBRARIES}
)

#target_link_libraries(TSDBLIB ${TCMALLOC_LIB})
target_link_libraries(TSDBLIB ${PROTOBUF_LIBRARIES})
target_link_libraries(TSDBLIB
  profiler
)
# target_link_libraries(TSDBLIB
#   tcmalloc
# )

target_link_libraries(TSDBLIB
  jemalloc
)
#target_link_libraries(TSDBLIB gRPC::grpc++)

include_directories(/usr/include/yaml-cpp)
target_link_libraries(TSDBLIB
  yaml-cpp
)

add_executable(leveldbutil
  "${CMAKE_SOURCE_DIR}/leveldb/db/leveldbutil.cc"
)
target_link_libraries(leveldbutil TSDBLIB)

enable_testing()

# Prevent overriding the parent project's compiler/linker settings on Windows.
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
set(install_gtest OFF)
set(install_gmock OFF)
set(build_gmock ON)

# This project is tested using GoogleTest.
set(BUILD_SHARED_LIBS_SAVED "${BUILD_SHARED_LIBS}")
set(BUILD_SHARED_LIBS OFF)
add_subdirectory("${CMAKE_SOURCE_DIR}/leveldb/third_party/googletest")
set(BUILD_SHARED_LIBS "${BUILD_SHARED_LIBS_SAVED}")

# GoogleTest triggers a missing field initializers warning.
if(LEVELDB_HAVE_NO_MISSING_FIELD_INITIALIZERS)
  set_property(TARGET gtest
      APPEND PROPERTY COMPILE_OPTIONS -Wno-missing-field-initializers)
  set_property(TARGET gmock
      APPEND PROPERTY COMPILE_OPTIONS -Wno-missing-field-initializers)
endif(LEVELDB_HAVE_NO_MISSING_FIELD_INITIALIZERS)

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/third_party)
include(FetchContent)
function(add_dep NAME GIT_URL GIT_TAG)
    string(TOLOWER "${NAME}" NAME_LOWER)
    FetchContent_Declare(${NAME})
    if (NOT ${NAME}_POPULATED)
        FetchContent_Populate(${NAME}
                QUIET
                SOURCE_DIR ${CMAKE_BINARY_DIR}/_deps/src/${NAME}
                BINARY_DIR ${CMAKE_BINARY_DIR}/_deps/build/${NAME}
                SUBBUILD_DIR ${CMAKE_BINARY_DIR}/_deps/sub/${NAME}
                GIT_REPOSITORY ${GIT_URL}
                GIT_TAG ${GIT_TAG}
                )
        
        set(DEP_CUSTOM_CMAKELISTS ${PROJECT_SOURCE_DIR}/third_party/masstree/${NAME}_CMakeLists.txt)
        if (EXISTS ${DEP_CUSTOM_CMAKELISTS})
            file(COPY ${DEP_CUSTOM_CMAKELISTS} DESTINATION ${${NAME_LOWER}_SOURCE_DIR})
            file(RENAME ${${NAME_LOWER}_SOURCE_DIR}/${NAME}_CMakeLists.txt ${${NAME_LOWER}_SOURCE_DIR}/CMakeLists.txt)
            message(STATUS "Copied ${DEP_CUSTOM_CMAKELISTS} -> ${${NAME_LOWER}_SOURCE_DIR}/CMakeLists.txt.")
        endif ()
        unset(DEP_CUSTOM_CMAKELISTS)

        add_subdirectory(${${NAME_LOWER}_SOURCE_DIR} ${${NAME_LOWER}_BINARY_DIR})
    endif ()

    execute_process(COMMAND git log -1 --format=%cd --date=short
            WORKING_DIRECTORY ${${NAME_LOWER}_SOURCE_DIR}
            OUTPUT_VARIABLE GIT_LAST_COMMIT)
    string(STRIP "${GIT_LAST_COMMIT}" GIT_LAST_COMMIT)

    message(STATUS "[FOUND] ${NAME} (${GIT_URL} ${GIT_TAG} ${GIT_LAST_COMMIT})")
endfunction()

function(add_off_dep NAME LOCAL_PATH)
    string(TOLOWER "${NAME}" NAME_LOWER)

    set(${NAME_LOWER}_SOURCE_DIR ${LOCAL_PATH})
    set(${NAME_LOWER}_BINARY_DIR ${CMAKE_BINARY_DIR}/_deps/build/${NAME})

    if (NOT EXISTS ${${NAME_LOWER}_SOURCE_DIR})
        message(FATAL_ERROR "Dependency ${NAME} not found at ${LOCAL_PATH}. Please ensure it is cloned.")
    endif ()

    set(DEP_CUSTOM_CMAKELISTS ${PROJECT_SOURCE_DIR}/third_party/masstree_wrapper/${NAME}_CMakeLists.txt)
    if (EXISTS ${DEP_CUSTOM_CMAKELISTS})
        file(COPY ${DEP_CUSTOM_CMAKELISTS} DESTINATION ${${NAME_LOWER}_SOURCE_DIR})
        file(RENAME ${${NAME_LOWER}_SOURCE_DIR}/${NAME}_CMakeLists.txt ${${NAME_LOWER}_SOURCE_DIR}/CMakeLists.txt)
        message(STATUS "Copied ${DEP_CUSTOM_CMAKELISTS} -> ${${NAME_LOWER}_SOURCE_DIR}/CMakeLists.txt.")
    endif ()
    unset(DEP_CUSTOM_CMAKELISTS)

    add_subdirectory(${${NAME_LOWER}_SOURCE_DIR} ${${NAME_LOWER}_BINARY_DIR})

    execute_process(COMMAND git log -1 --format=%cd --date=short
            WORKING_DIRECTORY ${${NAME_LOWER}_SOURCE_DIR}
            OUTPUT_VARIABLE GIT_LAST_COMMIT)
    string(STRIP "${GIT_LAST_COMMIT}" GIT_LAST_COMMIT)

    message(STATUS "[FOUND] ${NAME} (Local Path: ${LOCAL_PATH}, Last Commit: ${GIT_LAST_COMMIT})")
endfunction()

# add_dep(masstree https://github.com/kohler/masstree-beta.git master)
add_off_dep(masstree ${PROJECT_SOURCE_DIR}/third_party/masstree_wrapper/masstree)


function(tsdb_test test_file)
  get_filename_component(test_target_name "${test_file}" NAME_WE)

  add_executable("${test_target_name}" "")
  target_sources("${test_target_name}"
    PRIVATE
      "${PROJECT_BINARY_DIR}/${LEVELDB_PORT_CONFIG_DIR}/port_config.h"
      "${CMAKE_SOURCE_DIR}/leveldb/util/testutil.cc"
      "${CMAKE_SOURCE_DIR}/leveldb/util/testutil.h"

      "${test_file}"
  )
  target_link_libraries("${test_target_name}" TSDBLIB gmock gtest ${GTEST_LIBRARIES})
  target_compile_definitions("${test_target_name}"
    PRIVATE
      ${LEVELDB_PLATFORM_NAME}=1
  )
  #if("${test_file}" STREQUAL "TreeSeries/slab_management_test.cpp")
    include_directories(${CMAKE_CURRENT_SOURCE_DIR}/third_party)
    list(APPEND MASSTREEWRAPPER_INCLUDE_DIRECTORIES "${CMAKE_BINARY_DIR}/_deps/src/")
    list(APPEND MASSTREEWRAPPER_LINK_OPTIONS "-pthread")
    target_include_directories("${test_target_name}" PUBLIC "${MASSTREEWRAPPER_INCLUDE_DIRECTORIES}")
    target_link_libraries("${test_target_name}"  masstree)
    target_link_options("${test_target_name}" PUBLIC "${MASSTREEWRAPPER_LINK_OPTIONS}")
 # endif()
  add_test(NAME "${test_target_name}" COMMAND "${test_target_name}")
endfunction(tsdb_test)
tsdb_test("chunk/chunk_test.cc")
tsdb_test("compact/compact_test.cc")
tsdb_test("disk/log_manager_test.cc")
tsdb_test("head/head_test.cc")
tsdb_test("index/index_test.cc")
tsdb_test("index/postings_test.cc")
tsdb_test("index/mem_postings_test2.cc")
tsdb_test("index/ThmapPostings_test.cpp")
tsdb_test("leveldb/db/log_test.cc")
tsdb_test("mem/go_art_bench.cc")
tsdb_test("mem/go_art_test.cc")
tsdb_test("mem/inverted_index_test.cc")
tsdb_test("mem/inverted_index_bench.cc")
tsdb_test("mem/mem_postings_test.cc")
tsdb_test("mem/mem_postings_bench.cc")
tsdb_test("mem/mergeset_test.cc")
tsdb_test("mem/mergeset_version_edit_test.cc")
tsdb_test("mem/mergeset_version_set_test.cc")
tsdb_test("third_party/art_test.cc")
tsdb_test("third_party/art_optlock_test.cc")
tsdb_test("third_party/art_optlock_epoch_test.cc")
tsdb_test("third_party/persistent_cedarpp_test.cc")
tsdb_test("third_party/lfqueue_test.cc")

tsdb_test("TreeSeries/slab_management_test.cpp")
tsdb_test("TreeSeries/tree_series_test.cpp")
tsdb_test("mem/indirection_mapping_test.cpp")
tsdb_test("head/head_test2.cpp")
tsdb_test("head/tree_head_test.cpp")
tsdb_test("head/SpanHeadTest.cpp")
tsdb_test("db/RemoteStorage.cpp")
tsdb_test("db/DMiSeries_test.cpp")
tsdb_test("parallel_wal/parallel_wal_test.cpp")

tsdb_test("head/tree_head_query_test.cpp")
tsdb_test("querier/tsdb_tree_querier_test.cpp")
tsdb_test("querier/SpanQuerierTest.cpp")
tsdb_test("testutil/label_generator_test.cpp")
tsdb_test("testutil/memory_usage_test.cpp")
tsdb_test("testutil/config_test.cpp")

target_link_libraries(config_test yaml-cpp)

add_subdirectory(third_party)
add_subdirectory(test)