# include_directories(${CMAKE_CURRENT_SOURCE_DIR}/third_party)
# include(FetchContent)
# function(add_dep NAME GIT_URL GIT_TAG)
#     string(TOLOWER "${NAME}" NAME_LOWER)
#     FetchContent_Declare(${NAME})
#     if (NOT ${NAME}_POPULATED)
#         FetchContent_Populate(${NAME}
#                 QUIET
#                 SOURCE_DIR ${CMAKE_BINARY_DIR}/_deps/src/${NAME}
#                 BINARY_DIR ${CMAKE_BINARY_DIR}/_deps/build/${NAME}
#                 SUBBUILD_DIR ${CMAKE_BINARY_DIR}/_deps/sub/${NAME}
#                 GIT_REPOSITORY ${GIT_URL}
#                 GIT_TAG ${GIT_TAG}
#                 )
        
#         set(DEP_CUSTOM_CMAKELISTS ${PROJECT_SOURCE_DIR}/third_party/masstree/${NAME}_CMakeLists.txt)
#         if (EXISTS ${DEP_CUSTOM_CMAKELISTS})
#             file(COPY ${DEP_CUSTOM_CMAKELISTS} DESTINATION ${${NAME_LOWER}_SOURCE_DIR})
#             file(RENAME ${${NAME_LOWER}_SOURCE_DIR}/${NAME}_CMakeLists.txt ${${NAME_LOWER}_SOURCE_DIR}/CMakeLists.txt)
#             message(STATUS "Copied ${DEP_CUSTOM_CMAKELISTS} -> ${${NAME_LOWER}_SOURCE_DIR}/CMakeLists.txt.")
#         endif ()
#         unset(DEP_CUSTOM_CMAKELISTS)

#         add_subdirectory(${${NAME_LOWER}_SOURCE_DIR} ${${NAME_LOWER}_BINARY_DIR})
#     endif ()

#     execute_process(COMMAND git log -1 --format=%cd --date=short
#             WORKING_DIRECTORY ${${NAME_LOWER}_SOURCE_DIR}
#             OUTPUT_VARIABLE GIT_LAST_COMMIT)
#     string(STRIP "${GIT_LAST_COMMIT}" GIT_LAST_COMMIT)

#     message(STATUS "[FOUND] ${NAME} (${GIT_URL} ${GIT_TAG} ${GIT_LAST_COMMIT})")
# endfunction()

# function(add_off_dep NAME LOCAL_PATH)
#     string(TOLOWER "${NAME}" NAME_LOWER)

#     # 设置依赖的源代码目录和构建目录
#     set(${NAME_LOWER}_SOURCE_DIR ${LOCAL_PATH})
#     set(${NAME_LOWER}_BINARY_DIR ${CMAKE_BINARY_DIR}/_deps/build/${NAME})
    
#     # 检查目标路径是否存在
#     if (NOT EXISTS ${${NAME_LOWER}_SOURCE_DIR})
#         message(FATAL_ERROR "Dependency ${NAME} not found at ${LOCAL_PATH}. Please ensure it is cloned.")
#     endif ()

#     # 处理自定义的 CMakeLists.txt
#     set(DEP_CUSTOM_CMAKELISTS ${PROJECT_SOURCE_DIR}/third_party/masstree_wrapper/${NAME}_CMakeLists.txt)
#     if (EXISTS ${DEP_CUSTOM_CMAKELISTS})
#         file(COPY ${DEP_CUSTOM_CMAKELISTS} DESTINATION ${${NAME_LOWER}_SOURCE_DIR})
#         file(RENAME ${${NAME_LOWER}_SOURCE_DIR}/${NAME}_CMakeLists.txt ${${NAME_LOWER}_SOURCE_DIR}/CMakeLists.txt)
#         message(STATUS "Copied ${DEP_CUSTOM_CMAKELISTS} -> ${${NAME_LOWER}_SOURCE_DIR}/CMakeLists.txt.")
#     endif ()
#     unset(DEP_CUSTOM_CMAKELISTS)

#     # 添加子目录到构建系统
#     add_subdirectory(${${NAME_LOWER}_SOURCE_DIR} ${${NAME_LOWER}_BINARY_DIR})

#     # 获取最后一次提交日期
#     execute_process(COMMAND git log -1 --format=%cd --date=short
#             WORKING_DIRECTORY ${${NAME_LOWER}_SOURCE_DIR}
#             OUTPUT_VARIABLE GIT_LAST_COMMIT)
#     string(STRIP "${GIT_LAST_COMMIT}" GIT_LAST_COMMIT)

#     # 打印状态消息
#     message(STATUS "[FOUND] ${NAME} (Local Path: ${LOCAL_PATH}, Last Commit: ${GIT_LAST_COMMIT})")
# endfunction()

#在线拉取用这个
# add_dep(masstree https://github.com/kohler/masstree-beta.git master)
# 离线拉取用这个
# add_off_dep(masstree ${PROJECT_SOURCE_DIR}/third_party/masstree_wrapper/masstree)



set(gtest_disable_pthreads ON)

list(APPEND MASSTREEWRAPPER_INCLUDE_DIRECTORIES "${CMAKE_BINARY_DIR}/_deps/src/")
list(APPEND MASSTREEWRAPPER_LINK_OPTIONS "-pthread")

add_executable(masstree_test ${PROJECT_SOURCE_DIR}/test/masstree_test/masstree_test.cpp)

target_include_directories(masstree_test PUBLIC "${MASSTREEWRAPPER_INCLUDE_DIRECTORIES}")
  target_link_libraries(masstree_test PUBLIC masstree)
  target_link_options(masstree_test PUBLIC "${MASSTREEWRAPPER_LINK_OPTIONS}")

target_compile_definitions(masstree_test PUBLIC PROJECT_ROOT="${PROJECT_SOURCE_DIR}")