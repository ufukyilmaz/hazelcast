cmake_minimum_required(VERSION 3.10)
project(hazelcast-pmem)

set(CMAKE_C_STANDARD 11)
set(LIBRARY_OUTPUT_PATH ${CMAKE_CURRENT_LIST_DIR}/linux-x86_64 CACHE PATH "Build directory" FORCE)
set(CMAKE_SOURCE_PATH ${CMAKE_CURRENT_LIST_DIR}/src CACHE PATH "Source directory" FORCE)
set(LIB_PATH ${CMAKE_CURRENT_LIST_DIR}/linux-x86_64 CACHE PATH "Source directory" FORCE)
set(THREADS_PREFER_PTHREAD_FLAG ON)
set(CMAKE_BUILD_TYPE Release)
set(CMAKE_VERBOSE_MAKEFILE ON)
set(CMAKE_SKIP_BUILD_RPATH ON)

# direct dependencies shipped with libhazelcast-pmem.so
find_library(MEMKIND libmemkind.so.0 ${LIBRARY_OUTPUT_PATH} NO_DEFAULT_PATH)
find_library(PMEM libpmem.so.1 ${LIBRARY_OUTPUT_PATH} NO_DEFAULT_PATH)
find_library(NUMA libnuma.so.1 ${LIBRARY_OUTPUT_PATH} NO_DEFAULT_PATH)

find_package(Threads REQUIRED)

include_directories(.)
include_directories(include)
include_directories($ENV{JAVA_HOME}/include)
include_directories($ENV{JAVA_HOME}/include/linux)

add_library(hazelcast-pmem SHARED
        ${CMAKE_SOURCE_PATH}/com_hazelcast_internal_memory_impl_MemkindHeap.c
        ${CMAKE_SOURCE_PATH}/com_hazelcast_internal_memory_impl_MemkindHeap.h
        ${CMAKE_SOURCE_PATH}/com_hazelcast_internal_memory_impl_NUMA.c
        ${CMAKE_SOURCE_PATH}/com_hazelcast_internal_memory_impl_NUMA.h
        ${CMAKE_SOURCE_PATH}/hz_heap.c
        ${CMAKE_SOURCE_PATH}/hz_heap.h
        ${CMAKE_SOURCE_PATH}/util.c
        ${CMAKE_SOURCE_PATH}/util.h)

# set RUNPATH to $ORIGIN to look for the direct dependencies in the folder containing libhazelcast-pmem
set_target_properties(hazelcast-pmem PROPERTIES LINK_FLAGS "-Wl,-rpath,$ORIGIN")
target_link_libraries(hazelcast-pmem PUBLIC ${MEMKIND} ${NUMA} ${THREADS} ${PMEM} ${CMAKE_DL_LIBS})

# add libnuma as direct dependency to libhazelcast-pmem to load them from the same folder where libhazelcast-pmem.so resides
add_custom_command(TARGET hazelcast-pmem POST_BUILD
        COMMAND patchelf --add-needed libnuma.so.1 ${LIBRARY_OUTPUT_PATH}/libhazelcast-pmem.so)

# add the tests folder with the unit tests
add_subdirectory(tests)
