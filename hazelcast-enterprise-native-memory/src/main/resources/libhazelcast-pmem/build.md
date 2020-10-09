### Intro

This directory contains the sources and native libraries that are required for
building the Hazelcast native shared library (`libhazelcast-pmem`) used for
accessing persistent memory. Both `libhazelcast-pmem` and its dependencies are
packaged into the Hazelcast Enterprise jar file.


### Directory structure
```
/
|
+-- include/: the used header files
|
+-- linux-x86_64/: the direct dependency shared libraries and also the build output for libhazelcast-pmem.so
|                  this folder is taken by the maven assembly script
|
+-- src/: the JNI library source and header files to be built
|
+-- tests/: the directory containing the tests that verify the library  
|
+-- build.md: this document
|
+-- CMakeLists.txt: the CMake configuration file
```

### Dependencies

The following 3rd party native libraries are used:
- `Memkind`: The heap manager backed by `jemalloc` that can be directed with the desired source
  for the allocation, such as persistent memory (in FS DAX and KMEM DAX modes), DRAM and 
  others.
- `libpmem`: This library is used only to verify if the directories provided for the 
  persistent memory based allocator are on a persistent memory device.
- `libnuma`: Used by Memkind. Libraries for controlling NUMA policy.
- `libpthread`: Used by Memkind. The POSIX thread library.
- `libc`: Used by all. Standard C library.
- `libdl`: Used by all. The dynamic linker.
- `ld-linux-x86-64`: Used by all. The dynamic linker. 

The following table shows
- ver: the version of the lib
- jar: if the lib is bundled into the EE jar
- sys: if the lib is resolved from the system
- java dep: if the lib is a dependency of the `java` executable

```
+-----------------+---------+-----+-----+----------+
|  lib name       |   ver   | jar | sys | java dep |
+-----------------+---------+-----+-----+----------+
| libmemkind      | 1.10.1  |  X  |     |          |
+-----------------+---------+-----+-----+----------+
| libpmem         | 1.8.1   |  X  |     |          |
+-----------------+---------+-----+-----+----------+
| libnuma         | 2.0.11  |  X  |     |          |
+-----------------+---------+-----+-----+----------+
| libpthread      | N/A     |     |  X  |     X    |
+-----------------+---------+-----+-----+----------+
| libc            | N/A     |     |  X  |     X    |
+-----------------+---------+-----+-----+----------+
| libdl           | N/A     |     |  X  |     X    |
+-----------------+---------+-----+-----+----------+
| ld-linux-x86-64 | N/A     |     |  X  |          |
+-----------------+---------+-----+-----+----------+
```

Note that the Memkind package in the repositories of the Linux distributions depend on `libdaxctl` as well, but we omit
this dependency. Please see [Building Memkind](#building-memkind) for more details on this.

### Building

NOTE: building should be done on a suitable machine, ideally not on a personal machine. All
other building guidelines and requirements should be met, such as the physical location of
the build machine. If unsure, please consult with the release master. 

The library can be built with CMake so that IDEs that support it (such as CLion) can be used for editing
and debugging the Hazelcast PMEM native library.

Prerequisites for building:
- Have a network connection, the build will download the Google Test library
- Install a recent version of `CMake`
- Install a C compiler and linker, such as `gcc`
- Install `patchelf`

NOTE: testing depends on glibc's mcheck feature that tracks malloc/free calls.

To build the Hazelcast shared library, the following commands need to be executed.
Create the build files required for building with `cmake`:
```
cmake .
```
Build `linux-x86_64/libhazelcast-pmem.so` and the `tests/hazelcast-pmem-tests` binary used for testing:
```
cmake --build .
```            
Run the tests (this step sets `LD_LIBRARY_PATH` to find the shared libraries):
```
cd tests
ctest . -VV
``` 

After these steps are done, the result `libhazelcast-pmem.so` should be pushed to the
`hazelcast-enterprise` repository to be included in the Hazelcast Enterprise jar by the 
release scripts.

### Manual build verification

Delete `linux-x86_64/libhazelcast-pmem.so`.
 
Execute `PersistentMemoryMallocTest` and observe it is failing with
```
java.lang.ExceptionInInitializerError
	...
	at com.hazelcast.internal.memory.impl.PersistentMemoryMalloc.<init>(PersistentMemoryMalloc.java:28)
	at com.hazelcast.internal.memory.impl.PersistentMemoryMallocFactory.create(PersistentMemoryMallocFactory.java:20)
	at com.hazelcast.internal.memory.impl.PersistentMemoryMallocTest.setup(PersistentMemoryMallocTest.java:48)
	...
Caused by: java.lang.RuntimeException: Cannot find native library at : libhazelcast-pmem/linux-x86_64/libhazelcast-pmem.so
	at com.hazelcast.internal.memory.impl.PersistentMemoryHeap.extractBundledLib(PersistentMemoryHeap.java:124)
	at com.hazelcast.internal.memory.impl.PersistentMemoryHeap.<clinit>(PersistentMemoryHeap.java:44)
	... 35 more
```

Build as described in the `Building` section.

Test with `readelf`:
```
$ readelf -d linux-x86_64/libhazelcast-pmem.so  | grep -i -E "needed|runpath"
 0x0000000000000001 (NEEDED)             Shared library: [libnuma.so.1]
 0x0000000000000001 (NEEDED)             Shared library: [libmemkind.so.0]
 0x0000000000000001 (NEEDED)             Shared library: [libpmem.so.1]
 0x0000000000000001 (NEEDED)             Shared library: [libc.so.6]
 0x000000000000001d (RUNPATH)            Library runpath: [$ORIGIN]
```
NOTE that `RUNPATH` must contain `$ORIGIN` and only that.
Verify that all direct dependencies are added as `NEEDED`. 

Test with `ldd` should produce a similar output:
```
$ ldd -r linux-x86-64/libhazelcast-pmem.so
        linux-vdso.so.1 (0x00007ffdef76f000)
        libnuma.so.1 => <hazelcast-enterprise-root>/hazelcast-enterprise-native-memory/src/main/resources/libhazelcast-pmem/linux-x86_64/libnuma.so.1 (0x00007f34db7db000)
        libmemkind.so.0 => <hazelcast-enterprise-root>/hazelcast-enterprise-native-memory/src/main/resources/libhazelcast-pmem/linux-x86_64/libmemkind.so.0 (0x00007f34db51e000)
        libpmem.so.1 => <hazelcast-enterprise-root>/hazelcast-enterprise-native-memory/src/main/resources/libhazelcast-pmem/linux-x86_64/libpmem.so.1 (0x00007f34dbdc1000)
        libc.so.6 => /lib/x86_64-linux-gnu/libc.so.6 (0x00007f34db12d000)
        /lib64/ld-linux-x86-64.so.2 (0x00007f34dbbeb000)
        libdl.so.2 => /lib/x86_64-linux-gnu/libdl.so.2 (0x00007f34dab0b000)
        libpthread.so.0 => /lib/x86_64-linux-gnu/libpthread.so.0 (0x00007f34da8ec000)
```
NOTE that there are no unresolved symbols.

Execute `PersistentMemoryMallocTest` again and observe it is passing now.

### Upgrading Memkind

The 1.10.0 version of the Memkind library introduces the support for the KMEM DAX mode. To support that mode fully, it 
requires the additional `libdaxctl` library that brings other dependencies with it:
- `libuuid`: Used by `libdaxctl`. Universally Unique ID library.
- `libkmod`: Used by `libdaxctl`. Library for handling kernel modules.

While these dependencies are common on recent systems, their existence cannot be guaranteed. This is especially true in 
containers where adding `libkmod` is probably not a best practice. These libraries can be bundled into the enterprise jar
too, but since the KMEM DAX mode is kind of an exotic mode, we don't do this currently to keep the size of the binaries 
at a bare minimum. To prevent linking issues caused by missing these libraries, we package a custom Memkind build that
is built with `--enable-daxctl=no` and we don't distribute `libdaxctl` in our enterprise jar too. The JNI wrapper and 
the Java code supports this mode and it can be enabled by forcing Memkind (and its dependencies) to be loaded from a 
location outside of the Hazelcast Enterprise jar. This can be done by loading Memkind with setting `LD_PRELOAD` and 
configuring Hazelcast to use that.     

#### Building Memkind
Execute the following to build Memkind.
```
git clone https://github.com/memkind/memkind.git
cd memkind
git fetch --tags
git checkout <version tag such as: v1.10.0>
./autogen.sh
./configure --prefix=<absolute_install_path> --enable-daxctl=no
make
make install
strip <absolute_install_path>/lib/libmemkind.so.0.0.1
cp <absolute_install_path>/lib/libmemkind.so.0.0.1 <hazelcast-enterprise-root>/hazelcast-enterprise-native-memory/src/main/resources/libhazelcast-pmem/linux-x86_64/libmemkind.so.0
cp <absolute_install_path>/include/memkind.h <hazelcast-enterprise-root>/hazelcast-enterprise-native-memory/src/main/resources/libhazelcast-pmem/include
cp <absolute_install_path>/include/memkind_deprecated.h <hazelcast-enterprise-root>/hazelcast-enterprise-native-memory/src/main/resources/libhazelcast-pmem/include
```
After these steps `libhazelcast-pmem` needs to be rebuilt. 

##### Notes
The configuration option `--prefix` and `make install` can be omitted. That case the library built can be found in the 
`.libs` folder. 

Striping the .so file is required to remove the symbols from it. The built .so is otherwise too big. Please make sure 
that only `strip`'ed .so is pushed into the EE repository.
