cmake_minimum_required(VERSION 3.10)
project(hazelcast-pmem-tests)

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_CURRENT_LIST_DIR}/bin CACHE PATH "Build directory" FORCE)
set(CMAKE_LIBRARY_PATH ${CMAKE_CURRENT_LIST_DIR}/libx CACHE PATH "Build directory" FORCE)

include(CTest)

# Download and unpack googletest at configure time
configure_file(CMakeLists.txt.googletest googletest-download/CMakeLists.txt)
execute_process(COMMAND ${CMAKE_COMMAND} -G "${CMAKE_GENERATOR}" .
        RESULT_VARIABLE result
        WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/googletest-download)
if (result)
    message(FATAL_ERROR "CMake step for googletest failed: ${result}")
endif ()
execute_process(COMMAND ${CMAKE_COMMAND} --build .
        RESULT_VARIABLE result
        WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/googletest-download)
if (result)
    message(FATAL_ERROR "Build step for googletest failed: ${result}")
endif ()

# Prevent overriding the parent project's compiler/linker
# settings on Windows
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)

# Add googletest directly to our build. This defines
# the gtest and gtest_main targets.
add_subdirectory(${CMAKE_CURRENT_BINARY_DIR}/googletest-src
        ${CMAKE_CURRENT_BINARY_DIR}/googletest-build
        EXCLUDE_FROM_ALL)

add_executable(hazelcast-pmem-tests
        src/HzHeapTests.cpp
        ${CMAKE_SOURCE_PATH}/hz_heap.c
        ${CMAKE_SOURCE_PATH}/hz_heap.h)
target_link_libraries(hazelcast-pmem-tests PUBLIC ${MEMKIND} ${NUMA} ${THREADS} ${PMEM} ${CMAKE_DL_LIBS} mcheck gtest gtest_main)
add_test(NAME hazelcast-pmem-tests COMMAND hazelcast-pmem-tests WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY})
set_tests_properties(hazelcast-pmem-tests PROPERTIES ENVIRONMENT LD_LIBRARY_PATH=${LIB_PATH})
