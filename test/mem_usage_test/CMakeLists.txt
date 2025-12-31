list(APPEND MASSTREEWRAPPER_LINK_OPTIONS "-pthread")
add_executable(
        mem_usage_test

        ${PROJECT_SOURCE_DIR}/test/mem_usage_test/mem_usage_test.cpp
)
target_link_options(mem_usage_test.cpp PUBLIC "${MASSTREEWRAPPER_LINK_OPTIONS}")

