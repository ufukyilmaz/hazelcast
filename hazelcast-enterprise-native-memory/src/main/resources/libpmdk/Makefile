# PMDK project https://pmem.io/pmdk/ is a collection of native libraries to work with persistent memory.
# Hazelcast uses libpmem and libvmem from PMDK.
# We compile the following libraries and headers from the project:
# libpmem.a (static lib) + libpmem.h
# libvmem.a (static lib) + libpvmem.h
# with our JNI wrapper :
# com_hazelcast_internal_memory_impl_PersistentMemoryHeap.h
# com_hazelcast_internal_memory_impl_PersistentMemoryHeap.c
# into a single shared library: libpmdk.so

CC = gcc
ROOT_DIR:=$(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))
JNI_INCLUDES = $(JAVA_HOME)/include $(JAVA_HOME)/include/linux
CFLAGS = -O3 -DNDEBUG -fPIC
LINK_FLAGS = -fPIC -O3 -DNDEBUG -shared -Wl,--whole-archive libpmem.a libvmem.a -Wl,--no-whole-archive
C_BUILD_DIR = build
LIB_TARGET_DIR = linux-x86_64
ALL_C_SOURCES = $(wildcard $(ROOT_DIR)/*.c)
ALL_OBJ = $(addprefix $(C_BUILD_DIR)/, $(notdir $(ALL_C_SOURCES:.c=.o)))
SO_FILE_NAME = libpmdk.so
LIBRARIES = $(addprefix $(C_BUILD_DIR)/, $(SO_FILE_NAME))


all: $(LIB_TARGET_DIR) $(LIBRARIES) clean

clean:
	rm -rf $(C_BUILD_DIR)

$(LIBRARIES): | $(C_BUILD_DIR)

$(C_BUILD_DIR)/%.so: $(ALL_OBJ)
	$(CC) -Wl,-soname,$@ -o $@ $(ALL_OBJ) $(LINK_FLAGS)
	mv $(C_BUILD_DIR)/$(SO_FILE_NAME) $(LIB_TARGET_DIR)/$(SO_FILE_NAME)

$(C_BUILD_DIR)/%.o: $(ROOT_DIR)/%.c
ifndef JAVA_HOME
	$(error JAVA_HOME not set)
endif
	$(CC) $(CFLAGS) $(addprefix -I, $(ROOT_DIR) $(JNI_INCLUDES)) -o $@ -c $<

$(C_BUILD_DIR):
	mkdir -p $(C_BUILD_DIR)

$(LIB_TARGET_DIR):
	mkdir -p $(LIB_TARGET_DIR)
