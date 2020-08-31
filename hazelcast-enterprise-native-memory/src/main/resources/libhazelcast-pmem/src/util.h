#include <jni.h>

#ifndef HAZELCAST_PMEM_UTIL_H
#define HAZELCAST_PMEM_UTIL_H

void throw_OOM(JNIEnv *env, size_t size);

void throw_io_exception(JNIEnv *env, const char *msg);

void throw_runtime_exception(JNIEnv *env, const char *msg);

#endif //HAZELCAST_PMEM_UTIL_H
