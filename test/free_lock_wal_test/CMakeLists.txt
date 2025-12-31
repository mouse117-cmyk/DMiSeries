


list(APPEND MASSTREEWRAPPER_LINK_OPTIONS "-pthread")
add_executable(
    free_lock_wal_test

    ${PROJECT_SOURCE_DIR}/test/free_lock_wal_test/free_lock_wal_test.cpp
    "${CMAKE_SOURCE_DIR}/parallel_wal/free_lock_wal.cpp"
)
target_link_options(free_lock_wal_test PUBLIC "${MASSTREEWRAPPER_LINK_OPTIONS}")
