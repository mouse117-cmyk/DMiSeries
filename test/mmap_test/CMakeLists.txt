cmake_minimum_required(VERSION 3.10)
project(mmap_test_project)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED True)

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

add_executable(
        mmap_test_project
        ${PROJECT_SOURCE_DIR}/mmap_test.cpp
)

find_package(Boost REQUIRED COMPONENTS iostreams filesystem system)

target_link_libraries(
        mmap_test_project
        ${Boost_IOSTREAMS_LIBRARY}
        ${Boost_FILESYSTEM_LIBRARY}
        ${Boost_SYSTEM_LIBRARY}
)