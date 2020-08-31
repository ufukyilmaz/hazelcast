#include "util.h"

void throw_OOM(JNIEnv *env, size_t size) {
    char errmsg[256];

    jclass exClass = (*env)->FindClass(env, "java/lang/OutOfMemoryError");

    sprintf(errmsg, "Failed to allocate %lu bytes!", size);
    (*env)->ThrowNew(env, exClass, errmsg);
}

void throw_io_exception(JNIEnv *env, const char *msg) {
    jclass exClass = (*env)->FindClass(env, "java/io/IOException");
    (*env)->ThrowNew(env, exClass, msg);
}

void throw_runtime_exception(JNIEnv *env, const char *msg) {
    jclass exClass = (*env)->FindClass(env, "java/lang/RuntimeException");
    (*env)->ThrowNew(env, exClass, msg);
}
