set(gtest_disable_pthreads ON)
set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)

add_executable(thmap_test ${PROJECT_SOURCE_DIR}/test/thmap_test/thmap_test.cpp)
target_include_directories(thmap_test PUBLIC ${PROJECT_SOURCE_DIR}/third_party/thmap)
target_link_libraries(thmap_test PUBLIC    ${PROJECT_SOURCE_DIR}/third_party/thmap/murmurhash.o ${PROJECT_SOURCE_DIR}/third_party/thmap/thmap.o)

#add_executable(t_stress ${PROJECT_SOURCE_DIR}/test/thmap_test/t_stress.c)
#target_include_directories(t_stress PUBLIC ${PROJECT_SOURCE_DIR}/third_party/thmap)
#target_link_libraries(t_stress PUBLIC   ${PROJECT_SOURCE_DIR}/third_party/thmap/murmurhash.o ${PROJECT_SOURCE_DIR}/third_party/thmap/thmap.o )

add_executable(t_thmap ${PROJECT_SOURCE_DIR}/test/thmap_test/t_thmap.c)
target_include_directories(t_thmap PUBLIC ${PROJECT_SOURCE_DIR}/third_party/thmap)
target_link_libraries(t_thmap PUBLIC   ${PROJECT_SOURCE_DIR}/third_party/thmap/murmurhash.o ${PROJECT_SOURCE_DIR}/third_party/thmap/thmap.o )