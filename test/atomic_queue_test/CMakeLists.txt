


list(APPEND MASSTREEWRAPPER_LINK_OPTIONS "-pthread")
add_executable(
    atomic_queue_test
    ${PROJECT_SOURCE_DIR}/test/atomic_queue_test/atomic_queue_test.cpp
)
target_link_libraries(atomic_queue_test pthread)
target_include_directories(atomic_queue_test PUBLIC ${PROJECT_SOURCE_DIR}/third_party/atomic_queue/include)

add_executable(
    multi_thread_test
        ${PROJECT_SOURCE_DIR}/test/atomic_queue_test/multi_thread_test.cpp
)
target_link_libraries(multi_thread_test pthread)
target_include_directories(multi_thread_test PUBLIC ${PROJECT_SOURCE_DIR}/third_party/atomic_queue/include)