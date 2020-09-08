# Optane DC - Use all the DIMMS on a system and HD Memory Affinity

### Table of Contents

+ [Background](#background)
  - [Description](#description)
  - [Terminology](#terminology)
+ [Functional Design](#functional-design)
    * [The Memkind Library](#the-memkind-library)
    * [The PMEM Operation Modes](#the-pmem-operation-modes)
      + [The FS DAX Mode](#the-fs-dax-mode)
      + [The KMEM DAX Mode](#the-kmem-dax-mode)
  - [Comparison of the FS DAX and the KMEM DAX Modes](#comparison-of-the-fs-dax-and-the-kmem-dax-modes)
    * [Kernel 5.1+ Availability](#kernel-51-availability)
+ [User Interaction](#user-interaction)
  - [API design and/or Prototypes](#api-design-andor-prototypes)
+ [Technical Design](#technical-design)
  - [Configuration](#configuration)
    * [Declarative Configuration](#declarative-configuration)
    * [Programmatic Configuration](#programmatic-configuration)
    * [Validating the Configuration](#validating-the-configuration)
  - [Memkind Integration](#memkind-integration)
    * [Overview](#overview)
    * [Dynamic and Static Kinds](#dynamic-and-static-kinds)
    * [Supported Kinds](#supported-kinds)
    * [Creating PMEM Kinds](#creating-pmem-kinds)
    * [Detecting PMEM](#detecting-pmem)
    * [Implementation Details](#implementation-details)
    * [Setting MEMKIND_HOG_MEMORY](#setting-memkind_hog_memory)
    * [Linking](#linking)
  - [PMEM Allocation Strategies](#pmem-allocation-strategies)
    * [The Round-robin Allocation Strategy](#the-round-robin-allocation-strategy)
    * [The NUMA-aware Allocation Strategy](#the-numa-aware-allocation-strategy)
    * [Allocation Overflowing](#allocation-overflowing)
    * [Heap Detection](#heap-detection)
  - [Performance Analysis](#performance-analysis)
  - [Future Improvement Ideas](#future-improvement-ideas)
    * [Remove the libpmem Dependency](#remove-the-libpmem-dependency)
    * [Hazelcast PMEM Allocator](#hazelcast-pmem-allocator)
    * [NUMA-aware Allocation Overflowing](#numa-aware-allocation-overflowing)
  - [Other Artifacts](#other-artifacts)

### Background
#### Description
Hazelcast 4.0 introduced support for Intel Optane DC persistent memory as an HD memory backend. The Optane DC modules are supported by Hazelcast in interleaved mode, which means one region (aka interleave set) per CPU socket. The OS sees these  regions as separate devices (`/dev/pmem0`, `/dev/pmem1`, ...) and therefore can be mounted as separate mount points in file system DAX mode that Hazelcast 4.0 uses. This fact is not reflected by the Hazelcast configuration, which allows only a single `persistent-memory-directory` to be set limiting the available HD memory to only one socket's Optane DC modules. This problem can be worked around by creating an LVM group spanning over all socket's Optane DC modules, however, it requires more work from the system administrators and it is less native from the Hazelcast perspective. To overcome this problem, Hazelcast 4.1 implements a solution native to Hazelcast that allows utilizing the whole Intel Optane DC PMM capacity as HD persistent memory.   

Hazelcast 4.1 comes with thread affinity support that enables certain Hazelcast managed threads to be bounded to one NUMA node. From this change, it is expected that the NUMA locality of Hazelcast will improve thus the performance penalty of accessing NUMA-remote memory is eliminated from the fastpaths. Since the persistent memory modules are installed in the memory DIMM slots and share the memory bus with the DRAM, NUMA locality matters for the PMEM access too. Therefore, PMEM allocations need to respect the NUMA node on which the allocator thread is running, and should allocate persistent memory from the same NUMA node. This is also implemented under the scope of the work documented in this document.

#### Terminology

|Term|Definition|
|---|---|
|**PMEM**| Abbreviation for Persistent MEMory. |
|**DAX**| Direct Access (DAX) is a mechanism defined by SNIA as part of the NVM Programming Model that provides byte-addressable loads and stores when working with a persistent memory-aware file system through direct MMU mappings. |
|**PMEM-aware filesystem**|	The persistent memory file system can detect whether or not there is DAX support in the kernel. If so, when an application opens a memory mapped file on this file system, it has direct access to the persistent region. (EXT4, XFS, NTFS).|
|**FS DAX**| The mode of the PMEM that provides direct access through a PMEM-aware file system. It can be configured with `ndctl --mode=fsdax` on Linux.|
|**KMEM DAX**| The mode of the PMEM that provides direct access without the need of using a PMEM-aware file system. Can be configured with `ndctl --mode=devdax...` and `daxctl reconfigure-device --mode=system-ram`... on Linux. In this mode, besides the DRAM capacity, the PMEM capacity is also visible for the OS as regular memory and applications can end up allocating from the PMEM memory too.| 
|**NUMA**|The Non-Uniform Memory Access is a memory design used in computer architectures where the sockets have dedicated memory slots.|
|**NUMA Node**|	NUMA nodes are separated resource groups by which the OS and the applications can optimize their tasks based on the cost of accessing the resources in the different groups. Typically, the CPU socket and the closest memory banks build a NUMA node, but a NUMA node may consist only of memory as well.|
|**NUMA Distance**|	Accessing resources - memory access, inter-thread communication - on a NUMA node other than the NUMA node the current thread is running on incurs a performance penalty. This fact is encoded into the NUMA distance, which is an abstract number specific to a concrete node-to-node relation. The NUMA distances are typically represented as an NxN table, where N is the number of the nodes.|
|**NUMA-local**| Accessing a resource that belongs to the same NUMA node as the current thread is running on.|
|**NUMA-remote**|The opposite of NUMA-local.|
|**NUMA-aware**|The characteristic of an application or algorithm that takes the NUMA layout and the distances into account and strives to achieve the best performance by using NUMA-local access where it is possible.|

### Functional Design

In 4.1 the change of replacing libvmem with the Memkind library allows us to choose between two different modes for accessing the PMEM. The following sections introduce the Memkind library and discuss the two available modes in detail. 

##### The Memkind Library
The libvmem native library that Hazelcast 4.0 uses has been discontinued by Intel in favor of the Memkind library, which is an extensible heap manager built on top of jemalloc. Given this change, we replace libvmem with Memkind. Memkind gives control to the programmers to choose which memory pool they allocate from making them able to create a partitioned or tiered heap. Additionally to the library change, Memkind enables us to use a new AppDirect volatile memory mode of Optane DC, the KMEM DAX mode that's going to be introduced later in this document. 

##### The PMEM Operation Modes
The Intel Optane DC PMM can be used in different operation modes. What this document is focusing on are two AppDirect volatile modes: FS DAX and KMEM DAX. In the following sections, we discuss these modes and give a concise comparison in the end. Both operation modes provide a volatile perception of the PMEM, making it byte-addressable and eliminate the performance obstacles from the path to the PMEM such as the virtual page cache, syscalls, interrupts and major page faults.

###### The FS DAX Mode
The FS DAX mode is what Hazelcast 4.0 uses. In this mode, the PMEM is accessed through memory-mapped file(s). Using this mode requires a PMEM-aware file system (EXT4 and XFS on Linux) and requires the PMEM device(s) to be mounted with the `-o dax` mount option. Since there are separate devices created for every NUMA node's Optane DC memory, utilizing the whole capacity can be done in the following two ways:

1. Configuring an LVM spanning over all NUMA nodes' Optane DC memory. With this approach, no changes are needed in the Hazelcast
 codebase apart from porting the native code from libvmem to Memkind. The downside is that NUMA-aware PMEM access cannot be implemented, therefore, the best possible performance cannot be achieved.
2. Allowing users to configure multiple persistent memory directory entries. The NUMA node ids need to be assigned to each of
 them, which can be done either by introducing a numa-node attribute for the persistent memory directory element as shown in  the following configuration or by discovering the NUMA layout either programmatically or by parsing the output of several commands.

Hazelcast 4.1 implements the second option, and inherently supports the first. 

#### The KMEM DAX Mode
The 5.1 version of the Linux kernel introduces support for the volatile usage of PMEM and it allows hotplugging PMEM as system memory during runtime. This feature is referred to as KMEM DAX. With this feature, the capacity of the PMEM is added to the DRAM capacity allowing the OS to use the PMEM capacity as system memory. Therefore, all applications can utilize PMEM without changing/recompiling them. While this is an advantage in many cases, it also means the memory pool from which a memory region is allocated is opaque for the applications. Since PMEM has greater latency than DRAM, this has disadvantages as well, especially for a JVM application, where - for example - the GC performance is in strong correlation with the memory latency. It needs to be highlighted that as opposed to the Intel Optane DC's Memory Mode, in this mode the DRAM is not acting as the last level cache for the PMEM. Even though the L1-L3 caches are still in use for PMEM, the greater PMEM latency further penalizes applications using random memory access patterns with great cache miss rates. Therefore, to achieve stable and the best Hazelcast performance the JVM must not allocate its pools from PMEM. In the next paragraphs, we will discuss how this can be achieved. 

Before presenting how advanced Linux and process configurations can be made for strong JVM performance and stability guarantees, it needs to be highlighted that in the typical applications of Hazelcast, where Hazelcast is running on dedicated hardware with enough DRAM available and no other memory-hungry application running on the machines, no further configuration is needed. In some other applications though that want to run Hazelcast on shared machines, some unwanted situations can occur from JVM performance issues to getting the JVM killed by the OS. The following paragraphs describe what the challenges are for such applications and how to handle them. 

To understand the behavior of a system using PMEM in KMEM DAX mode, thus to understand the performance characteristics of such a system, the way how Linux allocates memory on modern NUMA machines needs to be discussed at a high level. The Linux kernel and the core libraries built on that are NUMA-aware, including the memory allocator, which prefers allocating NUMA-local memory over allocating NUMA-remote memory. This preference is implemented by taking the NUMA distances into account and allocating memory from the NUMA node with the smallest NUMA distance. In order to provide the best performance possible, the Linux kernel allocates DRAM before allocating PMEM as long as there is enough DRAM capacity available. To support this, the hotplugged PMEM is seen as a separate NUMA node, with a distance greater than the DRAM of the given socket's NUMA node that the PMEM belongs to, but smaller than the distance of the remote sockets' DRAMs. This can be seen in the following example taken from our Optane lab machine:

```shell
[zoltan@server33 ~]$ numactl -H
available: 3 nodes (0-1,3)
node 0 cpus: 0 1 2 3 4 5 6 7 8 9 20 21 22 23 24 25 26 27 28 29
node 0 size: 385615 MB
node 0 free: 384853 MB
node 1 cpus: 10 11 12 13 14 15 16 17 18 19 30 31 32 33 34 35 36 37 38 39
node 1 size: 385554 MB
node 1 free: 372538 MB
node 3 cpus:
node 3 size: 768000 MB
node 3 free: 767991 MB
node distances:
node   0   1   3
  0:  10  21  28
  1:  21  10  17
  3:  28  17  10
```

In this example, node 1's PMEM is configured to KMEM DAX mode as node 3, while node 0's PMEM is still in FS DAX mode (not visible here). This makes the NUMA layout consisting of 3 nodes. As defined by the node distances table, the allocation preference for the threads running on node 0 and node 1 - the nodes with CPU - is the following:

* Node0: Node0 > Node1 > Node3
* Node1: Node1 > Node3 > Node0

What this means is that both node 0 and node 1 allocate from their local DRAM, before allocating from their PMEM. Continuing on this thought, the applications won't allocate PMEM as long as they don't run out from the DRAM capacity - assuming a perfect distribution of allocations between the NUMA nodes. This is important for the host JVM running Hazelcast, because this is effectively a soft-guarantee for it to allocate from DRAM only as long as the DRAM capacity is not drained. It can happen though that either the JVM, an external process or the kernel itself  (disk cache, page table, cached memory, etc) eats up the available DRAM capacity making the JVM to allocate from PMEM. See the following answer from Intel on why the described preference can't guarantee DRAM-only allocations for the JVM.

> While the kernel generally tends to allocate memory from the same NUMA node as the requesting cpu, it also tries to keep
 memory mostly full with page cache and other caches that will shrink under pressure. In practice means that local-DRAM will fill up first, but their is no mechanism to pressure caches to free up DRAM before spilling allocations to PMEM…. With the current upstream dax_kmem infrastructure the only way to avoid application allocations from landing in PMEM is to explicitly exclude them via numactl nodebind.

Here comes memory binding in the picture. With memory binding, we can define which NUMA nodes a process or a thread can allocate from. By running the JVM with "numactl -m 0,1 java ..." it is now guaranteed that it won't allocate from PMEM. The allocation preference now is this, which means DRAM only:

* Node0: Node0 > Node1
* Node1: Node1 > Node0

However, by running the JVM memory-bound the memory available for it is significantly less than the memory reported to the OS and therefore to the JVM. This can lead to situations when the DRAM runs out, but the JVM can still allocate memory that cannot be backed by physical memory. Once the JVM tries to touch such memory, the OOM killer can potentially kill the JVM process. There are two ways of dealing with this problem. Before discussing the possible solutions, we discuss why and how this problem occurs.

The processes running in Linux are allowed to allocate more memory than what is physically available on a machine. This is based on the realization that programs don't use all the memory they allocate, therefore, the physical memory cannot be fully utilized, unless the processes can allocate more than what's available, which is called overcommitting. To allow overcommitting, the OS actually just registers the demand for the memory allocation but doesn't allocate physical memory backing the allocated virtual memory. The actual physical memory allocation and the virtual to physical memory mapping is done later when the allocated memory is touched. This is how in the aforementioned case the JVM can't use the memory it allocated, which forces the OS to kill a process or processes by using the OOM killer to free up some memory. In the background, the OS assigns an OOM score to every process, based on which the OOM killer acts when needed. This OOM score can be read out from `/proc/PID/oom_score`. If there is memory pressure, the OOM killer kills the processes with the highest score until the situation becomes healthy again. The OOM score is based on process statistics, with several factors taken into consideration that can increase or decrease the score. For the JVM hosting Hazelcast using PMEM in KMEM DAX mode, the following two factors are decreasing the score:

* whether the process has been running a long time, or has used a lot of CPU time
* whether the process is making direct hardware access

These factors make the JVM to be killed less likely, while on the other hand, since the JVM is bound to NUMA nodes, the memory available to it is much less than the memory available for the other processes. The used memory/allowed memory ratio is factored into the OOM score that the kernel assigns to the JVM process. This increases the score for the JVM running memory-bound. (question) Does the memory allocated with the `MEMKIND_DAX_KMEM` kind affect this score? If so, the OOM score of the JVM will actually be great, since it allocates a huge amount of memory from the `PMEM`. (question) 

Now we will discuss how the situation can be mitigated and how guarantees can be given to the JVM to survive the situation. The obvious solution is disabling the OOM killer for the JVM, which can be done by setting `-1000` to `/proc/PID/oom_score_adj`. Setting negative values can be done only by the root though, which limits the applicability of this solution. The other option is disabling overcommitting system-wide and setting the available memory for all processes to a smaller value than the available DRAM capacity. To disable overcommitting `/proc/sys/vm/overcommit_memory` needs to be set to `2`, and a use-case dependent overcommit ratio needs to be set in `/proc/sys/vm/overcommit_ratio`. The `overcommit_ratio = floor(defensive_factor * (DRAM / (DRAM + PMEM + swap)))` formula can be used for calculating this ratio, where the `defensive_factor` is less than or equal to 100.  Calculating with 100 will limit the system-wide commit threshold by the amount of DRAM. This is safe in many systems, but it should be noted that the kernel's structures kept in memory are not limited by the overcommit ratio, which means calculating with 100 might not leave enough space for structures such as page cache. This can lead to system-wide performance problems and can trigger the OOM killer too. Therefore, no concrete value can be advised for the `defensive_factor` that works perfectly for every use case.

On top of these above, the JVM can be started with `-XX:+AlwaysPreTouch`, with fix heap size and young:old ratio. This makes the whole heap backed by physical memory by the time Hazelcast actually starts, therefore no JVM heap allocations happen during runtime. This does not affect the other JVM pools though, those can potentially still overflow to PMEM, unless the JVM process is memory bound.

#### Comparison of the FS DAX and the KMEM DAX Modes
Both modes offer direct PMEM access bypassing the kernel, but they are fundamentally different, which is summarized in the following table.

| | FS DAX | KMEM DAX |
|---|---|---|
|Method of allocating persistent memory|	Memory-mapped files created on a PMEM-aware file system|	Directly allocated from the persistent memory module|
|Direct PMEM access| Yes | Yes |
|Guaranteed exclusive PMEM access| Yes | No |
|Needs advanced sysadmin settings| No | Yes<sup>1</sup> |
|Security implications | The HD memory is visible through memory-mapped file(s)<sup>2</sup>|No security implications|
|Syscalls|	Rarely<sup>3</sup> |	No
|NUMA-awareness| No, should be implemented in the Hazelcast codebase|Yes|
|Minimum kernel version | N/A| Linux 5.1+ (Released in May 2019) |
|Origin of memory allocated with malloc | DRAM | DRAM or PMEM<sup>4</sup> |
|Checking PMEM availability | By using libpmem | By allocating memory in this mode|

<sup>1</sup> For systems running Hazelcast on dedicated machines with enough DRAM available, no advanced configuration is needed, since Hazelcast is the only memory-hungry application. In some others though, the settings might be necessary to guarantee JVM performance and survival.

<sup>2</sup> The backing memory-mapped files need to be guarded by properly set file system privileges and user-level separation to protect the HD memory from being accessed by external processes. The proper privileges are as restrictive as possible, ideally 0600. Hazelcast 4.0 creates its temp file with 0666 privilege, which allows access to other user's processes too.

<sup>3</sup> PMEM rarely needs to perform syscalls to manage the memory-mapped file (mmap, madvise). Intel's comment on this is below. The context is comparing the performance of the FS DAX and the KMEM DAX modes.
> There may be small performance differences coming from fact that managing mmap-ed file via FS DAX sometimes requires syscalls
 to kernel (mmap, madvise). Other then that, accessing  Intel Optane via FS DAX or KMEM DAX should be similar. We are currently working on allowing user to configure behavior in context of madvise calls on FS DAX (i.e. to allow use or forbid using this syscall).

<sup>4</sup> This can be at least impacted with OS and process-specific configuration, see the KMEM DAX section above.

##### Kernel 5.1+ Availability
In general, it can be said that the new releases of the mainstream Linux distributions are shipped/going to be shipped with Linux 5.3 or 5.4.

| Distro       | Version | Kernel | Release Date | Comment | 
|--------------|---------|--------|--------------|---------| 
| Alpine Linux | 3.11.0  | 5.4    | 2019-12-19   | HZ Cloud uses it for 4.0 |
| Amazon Linux |  2      | 4.19   | 2019-07-18   | |
| Debian       | 11      | 5.4    | TBA          | Debian has a ~bi-yearly release cadence, expected in 2021 |        
| Debian       | 10      | 4.19   | 2019-07-06   | |	
| Fedora	   | 31	     | 5.3	  | 2019-10-29   | |	
| Red Hat	   | 8.1     | 4.18	  | 2019-11-05	 | |
| Ubuntu	   | 19.0	 | 5.3	  | 2019-11-17	 | Available on AWS |
 
### User Interaction
#### API design and/or Prototypes
No public API change to be done for this work, apart from configuration changes, that depends on the chosen implementation mode. See the options shown in the sections discussing the different modes.

### Technical Design
In the following sections we discuss the implementation details of the support of Optane DCPMM in Hazelcast. 

#### Configuration
In 4.0 we supported configuring only a single directory for persistent memory, that we improve in 4.1. The improvement introduces a `persistent-memory` section under `native-memory` in the configuration that replaces the single String based directory setting that we had in 4.0. This change allows setting unlimited number of persistent memory directories as well as defining the NUMA node for each directory, enabling NUMA-awareness in eligible setups. Having a dedicated section for persistent memory also enables adding more advanced configuration in the future.   
      
While we improve the configuration, we need to keep backward compatibility to any existing configuration is still working without modifications. To meet this goal we keep the existing configuration as deprecated that leaves us with two options for defining the configuration for persistent memory.

##### Declarative Configuration
The following example shows an example for the XML configuration:
```xml
<hazelcast>
    <native-memory allocator-type="POOLED" enabled="true">
        <size unit="GIGABYTES" value="100" />
        <!-- Single directory as introduced in 4.0, deprecated -->
        <persistent-memory-directory>/mnt/pmem</persistent-memory-directory>
        <!-- Multiple directories and extensible since 4.1 -->
        <persistent-memory>
            <directories>
                <directory numa-node="0">/mnt/pmem0/</directory>
                <directory numa-node="1">/mnt/pmem1/</directory>
            </directories>
        </persistent-memory>
    </native-memory>
</hazelcast>
```

The YAML configuration identical to the above XML configuration: 
```yaml
hazelcast:
  native-memory:
    size:
      unit: GIGABYTES
      value: 100                          
    # Single directory as introduced in 4.0, deprecated 
    persistent-memory-directory: /mnt/pmem
    # Multiple directories and extensible since 4.1
    persistent-memory:
      directories:
        - directory: /mnt/pmem0
          numa-node: 0
        - directory: /mnt/pmem1
          numa-node: 1
```
##### Programmatic Configuration
The Java configuration reflects the above changes and introduces the following persistent memory configuration classes.

The public API of the `PersistentMemoryConfig` is as follows.
```java
/**
 * Configuration class for persistent memory devices (e.g. Intel Optane).
 */
public class PersistentMemoryConfig {
    /**
     * Constructs an instance with copying the fields of the provided
     * {@link PersistentMemoryConfig}.
     *
     * @param persistentMemoryConfig The configuration to copy
     * @throws NullPointerException if {@code persistentMemoryConfig} is {@code null}
     */
    public PersistentMemoryConfig(@Nonnull PersistentMemoryConfig persistentMemoryConfig);

    /**
     * Returns the persistent memory directory configurations to be used
     * to store memory structures allocated by native memory manager.
     * <p>
     * By default there are no configuration is set indicating that
     * volatile RAM is being used.
     *
     * @return the list of the persistent memory directory configurations
     */
    @Nonnull
    public List<PersistentMemoryDirectoryConfig> getDirectoryConfigs();

    /**
     * Sets the persistent memory directory configuration to the set of
     * directories provided in the {@code directoryConfigs} argument.
     * <p/>
     * If the specified directories are not unique either in the directories
     * themselves or in the NUMA nodes specified for them,
     * {@link InvalidConfigurationException} is thrown. Setting the NUMA
     * node on the subset of the configured directories while leaving
     * not set on others also results in {@link InvalidConfigurationException}.
     *
     * @param directoryConfigs The persistent memory directories to set
     * @return this {@link PersistentMemoryConfig} instance
     * @throws InvalidConfigurationException If the configured directories
     *                                       violate consistency or
     *                                       uniqueness checks.
     * @throws NullPointerException if {@code directoryConfigs} is {@code null}
     */
    public PersistentMemoryConfig setDirectoryConfigs(@Nonnull List<PersistentMemoryDirectoryConfig> directoryConfigs);

    /**
     * Adds the persistent memory directory configuration to be used to
     * store memory structures allocated by native memory manager.
     * <p/>
     * If the specified directories are not unique either in the directories
     * themselves or in the NUMA nodes specified for them,
     * {@link InvalidConfigurationException} is thrown. Setting the NUMA
     * node on the subset of the configured directories while leaving
     * not set on others also results in {@link InvalidConfigurationException}.
     *
     * @param directoryConfig the persistent memory directory configuration
     * @return this {@link PersistentMemoryConfig} instance
     * @throws InvalidConfigurationException If the configured directories
     *                                       violate consistency or
     *                                       uniqueness checks.
     * @throws NullPointerException if {@code directoryConfigs} is {@code null}
     */
    public PersistentMemoryConfig addDirectoryConfig(@Nonnull PersistentMemoryDirectoryConfig directoryConfig);
``` 

The public API of the `PersistentMemoryDirectoryConfig`:
```java
/**
 * Configuration class for directories that persistent memories are mounted at.
 */
public class PersistentMemoryDirectoryConfig {

    /**
     * Creates an instance with the {@code directory} specified.
     *
     * @param directory The directory where the persistent memory is
     *                  mounted at
     * @throws NullPointerException if {@code directory} is {@code null}
     */
    public PersistentMemoryDirectoryConfig(@Nonnull String directory);

    /**
     * Creates an instance with the {@code directory} and NUMA node specified.
     * <p/>
     * Note that setting {@code numaNode} to -1 on any of the configured
     * {@link PersistentMemoryDirectoryConfig} instances disables
     * NUMA-aware persistent memory allocation.
     *
     * @param directory The directory where the persistent memory is
     *                  mounted at
     * @param numaNode  The NUMA node that the persistent memory mounted
     *                  to the given directory is attached to.
     * @throws NullPointerException if {@code directory} is {@code null}
     */
    public PersistentMemoryDirectoryConfig(@Nonnull String directory, int numaNode);

    /**
     * Constructs an instance by copying the provided {@link PersistentMemoryDirectoryConfig}.
     *
     * @param directoryConfig The configuration to copy
     * @throws NullPointerException if {@code directoryConfig} is {@code null}
     */
    public PersistentMemoryDirectoryConfig(@Nonnull PersistentMemoryDirectoryConfig directoryConfig);

    /**
     * Returns the directory of this {@link PersistentMemoryDirectoryConfig}.
     *
     * @return the directory
     */
    @Nonnull
    public String getDirectory();

    /**
     * Returns the {@code directory} of this {@link PersistentMemoryDirectoryConfig}.
     *
     * @param directory the directory to set
     * @throws NullPointerException if {@code directory} is {@code null}
     */
    public void setDirectory(@Nonnull String directory);

    /**
     * Returns the NUMA node the persistent memory mounted to the given
     * directory is attached to.
     *
     * @return the NUMA node of the persistent memory
     */
    public int getNumaNode();

    /**
     * Sets the NUMA node the persistent memory mounted to the given
     * directory is attached to.
     * <p/>
     * Note that setting {@code numaNode} to -1 on any of the configured
     * {@link PersistentMemoryDirectoryConfig} instances disables
     * NUMA-aware persistent memory allocation.
     *
     * @param numaNode the NUMA node to set
     */
    public void setNumaNode(int numaNode);

    /**
     * Returns if the NUMA node for the given persistent memory directory
     * is set.
     *
     * @return {@code true} if the NUMA node is set, {@code false} otherwise
     */
    public boolean isNumaNodeSet();
}
```

To support the new persistent memory configuration, we introduce the following changes to `NativeMemoryConfig`:
```java
public class NativeMemoryConfig {

    /**
     * Returns the persistent memory configuration this native memory
     * configuration uses.
     *
     * @return the persistent memory configuration
     */
    @Nonnull
    public PersistentMemoryConfig getPersistentMemoryConfig();

    /**
     * Sets the persistent memory configuration this native memory
     * configuration uses.
     *
     * @param persistentMemoryConfig The persistent memory configuration to use
     */
    public void setPersistentMemoryConfig(@Nonnull PersistentMemoryConfig persistentMemoryConfig);

    /**
     * Returns the persistent memory directory (e.g. Intel Optane) to be
     * used to store memory structures allocated by native memory manager.
     * If there are multiple persistent memory directories are defined in
     * {@link #persistentMemoryConfig}, an {@link IllegalStateException}
     * is thrown.
     *
     * @see PersistentMemoryConfig#getDirectoryConfigs()
     * @deprecated Since 4.1 multiple persistent memory directories are
     * supported. Please use {@link PersistentMemoryConfig#getDirectoryConfigs()}
     * instead.
     */
    @Deprecated
    @Nullable
    public String getPersistentMemoryDirectory();

    /**
     * Sets the persistent memory directory (e.g. Intel Optane) to be used
     * to store memory structures allocated by native memory manager. If
     * the {@link #persistentMemoryConfig} already contains directory
     * definition, it is overridden with the provided {@code directory}.
     *
     * @param directory the persistent memory directory
     * @return this {@link NativeMemoryConfig} instance
     * @see #getPersistentMemoryConfig()
     * @see PersistentMemoryConfig#addDirectoryConfig(PersistentMemoryDirectoryConfig)
     * @deprecated Since 4.1 multiple persistent memory directories are
     * supported. Please use {@link #setPersistentMemoryConfig(PersistentMemoryConfig)}
     * or {@link PersistentMemoryConfig#addDirectoryConfig(PersistentMemoryDirectoryConfig)}
     * instead.
     */
    @Nonnull
    @Deprecated
    public NativeMemoryConfig setPersistentMemoryDirectory(@Nullable String directory);
}
```
Note that we deprecate the API we introduced in 4.0 for setting the single directory.

##### Validating the Configuration
We have the following validation rules for the configuration:
* The directories have to be unique
* The NUMA nodes defined on the directories have to be unique or not set  
* If NUMA node is set at least on one directory, the NUMA nodes have to be set on all directories

If any of the above validation rules is violated we throw an `InvalidConfigurationException` with a detailed message that explains the violation.

#### Memkind Integration
We integrate with Memkind v1.10.1 that as of writing this document has not been released yet. The Memkind team expects it to be released in 2020 September. The version is important for the Hazelcast integration, because it comes with changes that reduce the shutdown times of Hazelcast significantly. In the following sections we discuss the aspects of the library that are relevant for Hazelcast.

We also develop a thin JNI layer named `libhazelcast-pmem.so` that wraps the Memkind (and some other) native library functionality and binds them with our internal Java API.   
       
##### Overview              
The Memkind library is a heap manager library offering a familiar malloc-like interface for the C programmers. The memkind API is different from malloc/realloc/free in that it adds a kind argument to the argument list of these functions. A Kind in the Memkind terminology is used to distinguish between memory pools such as DRAM with regular page size, DRAM with huge page size, persistent memory and others. With these kinds the users of Memkind can easily build tiered heaps.

Memkind uses jemalloc as its internal heap manager and maintains separate arenas for every kind.    
       
Examples of the Memkind heap management functions:
```c
void *memkind_malloc(memkind_t kind, size_t size);
void *memkind_realloc(memkind_t kind, void *ptr, size_t size);  
void memkind_free(memkind_t kind, void *ptr);
```       

##### Dynamic and Static Kinds
Memkind have static kinds that the users can use out of the box without any initialization of such kinds. Examples of these kinds:
* **MEMKIND_DEFAULT**: the kind to use for allocating DRAM backed memory with regular page size
* **MEMKIND_HUGETLB**: the kind to use for allocating DRAM backed memory with huge page size
* **MEMKIND_DAX_KMEM**: the kind to use for allocating PMEM backed memory if the PMEM is onlined as system memory   

An example of allocating memory with these kind is as easy as the following example that allocates 64MB of memory:
```c 
void *ptr = memkind_malloc(MEMKIND_DEFAULT, (size_t) 64*1024*1024);
```

The users can use dynamic kinds that need to be defined in runtime. The PMEM kinds is a dynamic kind that is special in the way that Memkind offers API for creating such kinds.  


##### Supported Kinds
* **PMEM**: the kind to use for allocating PMEM backed memory if the PMEM is used in FS DAX mode    
* **MEMKIND_DEFAULT**: the kind to use for allocating DRAM backed memory with regular page size
* **MEMKIND_HUGETLB**: the kind to use for allocating DRAM backed memory with huge page size
* **MEMKIND_DAX_KMEM**: the kind to use for allocating PMEM backed memory if the PMEM is onlined as system memory   

We have official configuration only for the PMEM kind, through the previously mentioned configuration options. The rest is currently available only with undocumented system arguments. This is because the `MEMKIND_DEFAULT` and `MEMKIND_HUGETLB` kinds are supported for testing and experimantation purpose, while officially supporting `MEMKIND_DAX_KMEM` mode is not yet decided. 
  
Using the kinds can be enabled with the following system arguments:
* **MEMKIND_DEFAULT**: `-Dhazelcast.hd.memkind=true`   
* **MEMKIND_HUGETLB**: `-Dhazelcast.hd.memkind.hugepages=true`   
* **MEMKIND_DAX_KMEM**: `-Dhazelcast.hd.memkind.dax.kmem=true`   
 
Setting any of these arguments makes all HD allocations to be served with the associated Memkind kind.  
 
###### Creating PMEM Kinds
Memkind offers the following two functions for managing the PMEM kinds:
 
```c
int memkind_create_pmem(const char *dir, size_t max_size, memkind_t *kind);
int memkind_destroy_kind(memkind_t kind);
```          

Note that `memkind_create_pmem` receives a `dir` argument that points to a directory where the persistent memory is configured. This means we create a dynamic PMEM kind for every directory defined in the Hazelcast configuration that we need to destroy during the Hazelcast instance shutdown. 
       
##### Detecting PMEM
Memkind doesn't offer any API for that, the PMEM kinds can be created on regular file systems, hence on regular disks. This is useful for testing, but can be a problem in production. Therefore, we need to detect in runtime if the given directory is on persistent memory. For that and only for that purpose we keep `libpmem`.           
       
##### Implementation Details       
As mentioned before, Memkind uses jemalloc internally that creates multiple arenas and inside the arenas multiple extents. The small sized allocations of the similar size (between the same power of two boundaries) are allocated from the same extent, the large allocations are allocated from their own extents, one extent for each allocation. How it is important for Hazelcast is that the PMEM kind is file backed. Memkind creates one file for every PMEM kind on the persistent memory devices that back all PMEM allocations by memory mapping the file in a PMEM-specific mode. That means all extents are file backed and creating/destroying them requires system calls. Creating an extent is normally a fast operation on persistent memory. Destroying an extent, however, is expensive as it is implemented with hole punching. Hole punching is a feature of the Linux kernel for making a file sparse. In short, hole punching zeroes out the affected region of the file and then detaches it from the file. All subsequent reads from the file for the affected region will return zeroes, but the region will no longer occupy space on the underlying persistent device.  
           
Destroying extents can happen in two cases. The first is as a result of freeing up the memory that makes jemalloc to think the extent should no longer be kept with the process. The second is if the jemalloc heap is destroyed. Since in Hazelcast we use POOLED memory, we free up the pages backing the HD allocator during shutdown, which means a very large number of parallel free calls, thus extent destroys. Since the aformentioned hole punching is done on a single file, the kernel serializes the parallel free calls that destroy the extents, resulting in very long shutdown times for Hazelcast. With 200GB of data we have seen timeouts after 15 mins in Simulator tests.

Memkind v1.10.1 introduces the `MEMKIND_HOG_MEMORY` environment variable that if set to 1 prevents releasing the already allocated persistent memory, which defers destroying the extents making freeing up the persistent memory a lot faster. In exchange, the cost of destroying the extents goes to destroying the kind. This is not required though, as the files backing the PMEM kinds are temporary files that are going to be deleted when the process exits, so no point in doing any file manipulation to clean up. This is a late change we requested from the Memkind team after they released v1.10.1-rc2. 
    
##### Setting MEMKIND_HOG_MEMORY          
As explained in the previous section, setting `MEMKIND_HOG_MEMORY=1` is crucial for the timely Hazelcast shutdown times. To ensure the users don't experience unexpectedly long shutdowns, Hazelcast sets this before the first interaction with the Memkind library. This is done in the native JNI code by calling `setenv()`. 
         
If the user already set this, we don't make an attempt to set it. This is a safe option if for any reason `setenv()` fails. 

If it is desired to disable `MEMKIND_HOG_MEMORY` for any reason, setting it by Hazelcast can be disabled by setting the environment variable `HZ_SET_HOG_MEMORY=0`.
         
##### Linking
It is an explicit goal of our Optane support to free up from our users from any system configuration. To do that, we need to ship Memkind and its dependencies together with Hazelcast. This is done by embedding these libraries into the Hazelcast Enterprise jar. To achieve that, we need to either statically link the Hazelcast JNI shared object against Memkind and it's dependencies or linking dynamically and then embedding the shared objects in the EE jar. 
   
In general, the main advantage of statically linked applications is that those are more portable as those don't expect any shared objects to be present on the machines they run on. The disadvantage is the same: the statically linked applications' dependencies are not replaceable without rebuilding the application. The latter is important for us, since if for any reason a customer wants to use its desired version of a dependency (for example for security reasons), it's not possible. Therefore, we have chosen dynamic linking.     

Dynamic linking is even easier, because we can just embed any officially distributed shared object and link against those. Static linking may require us to rebuild the library with specific flags (mostly `-fPIC`). In exchange, when dynamically loading a shared object, the dynamic linker tries to load all of its dependencies and fail if any of them cannot be found. This is relevant for us, because Memkind has multiple dependencies and some may not be available on all systems by default, and we also don't want to embed each. In the following we list the dependencies of Memkind and our `libhazelcast-pmem.so` to get a full understanding of the dependencies:  

* `Memkind`: The heap manager backed by `jemalloc` that can be directed with the desired source
  for the allocation, such as persistent memory (in FS DAX and KMEM DAX modes), DRAM and 
  others.
* `libpmem`: This library is used only to verify if the directories provided for the 
  persistent memory based allocator are on a persistent memory device.
* `libnuma`: Used by Memkind. Libraries for controlling NUMA policy.
* `libpthread`: Used by Memkind. The POSIX thread library.
* `libc`: Used by all. Standard C library.
* `libdl`: Used by all. The dynamic linker.
* `ld-linux-x86-64`: Used by all. The dynamic linker. 

The following table shows
* ver: the version of the lib
* jar: if the lib is bundled into the EE jar
* sys: if the lib can be safely resolved from the system
* java dep: if the lib is a dependency of the `java` executable
* necessary: if the lib is necessary for the FS DAX mode

|  lib name         |   ver   | jar | sys | java dep | necessary |
|-------------------|---------|-----|-----|----------|-----------|
| libpmem           | 1.8.1   |  X  |     |          |     X     |
| libmemkind        | 1.10.1  |  X  |     |          |     X     |
|   libnuma         | 2.0.11  |  X  |     |          |     X     |
|   libpthread      | N/A     |     |  X  |     X    |     X     |
|   libdaxctl       | 66+     | N/A |     |          |           |
|     libkmod       | N/A     |     |  X  |          |           |
|     libuuid       | N/A     |     |  X  |          |           |
| libc              | N/A     |     |  X  |     X    |     X     |
| libdl             | N/A     |     |  X  |     X    |     X     |
| ld-linux-x86-64   | N/A     |     |  X  |          |     X     |

As it can be seen, `libdaxctl` and its dependencies are not necessary for the FS DAX mode that we primarily offer. Those libraries are required only for the KMEM DAX mode, that we currently don't consider as a mainstream option. `libkmod` and `libuuid` are very common libraries on Linux systems, but we can't expect them in container environments. `libkmod` for example is not available in our CI docker image. Also, `libkmod` requires further dependencies. Luckily, Memkind can be built without its `libdaxctl` dependency, so we relax our dependencies to the following: 

|  lib name         |   ver   | jar | sys | java dep |
|-------------------|---------|-----|-----|----------|
| libpmem           | 1.8.1   |  X  |     |          |
| libmemkind        | 1.10.1  |  X  |     |          |
|   libnuma         | 2.0.11  |  X  |     |          |
|   libpthread      | N/A     |     |  X  |     X    |
| libc              | N/A     |     |  X  |     X    |
| libdl             | N/A     |     |  X  |     X    |
| ld-linux-x86-64   | N/A     |     |  X  |          |

Since many of the remaining libraries are direct dependencies of the `java` executable, we need to bundle only `libpmem`, `libmemkind` and `libnuma` in the Hazelcast Enterprise jar. `ld-linux-x86-64` is very essential on Linux systems and it is safe to expect it is present. 

That, however, means that we need to build Memkind ourselves. It is easy though. The exact steps can be found in the `build.md` document in the `libhazelcast-pmem` lib's folder, we don't include it here.

#### PMEM Allocation Strategies
As mentioned before, with Memkind we create multiple Memkind heaps (PMEM kinds) but the Hazelcast HD code can't differentiate between these heaps and interact with Memkind through `LibMalloc` implementations. For the PMEM kinds, the implementation is the `MemkindPmemMalloc` that is associated with multiple `MemkindHeap` instances, each representing a heap created a configured persistent memory directory. Hiding the multiple heaps as well as distributing the allocations between these heaps is the responsibility of the `MemkindPmemMalloc` implementation. This is done through the allocation strategies. In Hazelcast 4.1 we implement two allocation strategies that we discuss in the next sections.  These strategies are determined on thread basis and are cached statically for the allocator threads for the whole lifetime of the Hazelcast instance.  

##### The Round-robin Allocation Strategy
With this strategy, the allocation requests address the heaps in a round-robin fashion. Every allocation request advances a global sequence, which is used in the `heap_index = heap_sequence % heap_count` formula to determine which heap to allocate from. This allocation strategy can be seen as a software interleaving of the persistent memory DIMMs attached to the different sockets. The advantage is that assuming a balanced usage of the entries from the point of view of the PMEM heaps it lowers the latency for accessing the data allowing higher throughput. This strategy is the chosen on if the NUMA-aware strategy cannot be chosen. 
       
##### The NUMA-aware Allocation Strategy
The NUMA-aware strategy prefers allocating from the PMEM heap, that is NUMA-local to the NUMA node that the current allocator thread is bounded to. This implies that the allocator thread needs to be restricted to run on a single NUMA node's CPUs. This requires the application to configure the affinity system attributes as described in detail by the Thread affinity feature's design document. Typically, the operation threads should be distributed evenly across the NUMA nodes. An example configuration for that: `-Dhazelcast.operation.thread.affinity=[0-9,20-29]:20,[10-19,30-39]:20`. This configuration restricts all 40 operation threads to run on a single NUMA node on a dual-socket 40 core system, where node0's cpu set is `[0-9,20-29]` and node1's cpu set is `[10-19,30-39]`. 
      
Besides node-binding the operation threads, the `numa-node` attribute of the persistent memory directory should be set properly. If both is done, this allocation strategy is chosen automatically, without any further configuration.
       
The advantage of this is strategy is that all access to the PMEM backed memory blocks are NUMA-local accesses that lower the LLC misses, which increases the throughput of the members significantly.     

##### Allocation Overflowing
Since both strategies address a specific PMEM heap with the allocations, it may happen that the chosen heap cannot serve the allocation request, while on other heaps there may be enough unused capacity. To utilize the whole PMEM capacity, we implement allocation overflowing, which means allocating from any PMEM heap if the chosen one is full. This logic is implemented as simply iterating over all known heaps until one serves the allocation request. If none of the heaps can allocate the requested block size, an `OutOfMemoryError` is thrown as every `LibMalloc` implementation does. 

It needs be mentioned that only allocation requests can overflow, reallocation requests cannot. This is because realloc has the contract of keeping the data even if the reallocation call returns a new address. This is done by copying the data that case. In theory, we could do it by copying the data manually, but we know only the address of the memory block, its size we don't.
 
##### Heap Detection 
Since the allocations can be served from any PMEM heap, on freeing we need to address the right heap with the free and the reallocation requests. Tracking this though would come with either external (separate datastructure) or internal (end of memory block) fragmentation. Memkind offers this functionality both for `memkind_realloc()` and for `memkind_free()` with just leaving the kind `NULL`. It will force Memkind to lookup its internal structures and choose its right kind to serve the request. All it needs is to guarantee that the block behind the pointer was allocated with Memkind and that it is a valid pointer (wasn't freed before). If the memory block was allocated with a different allocator, was freed before or was allocated from an already destroyed kind the behavior is unspecified and it's very likely causes a segmentation fault, crashing the JVM. Since we know for sure that all memory blocks are allocated with Memkind, we need to ensure only that we don't double-free, which is the responsibility of the memory managers. If later we implement a tiered storage we need to take extra care of this though and possibly the safest option will be to use Memkind for every kind of memory allocation behind HD.  
     
#### Performance Analysis

The following table summarizes the results of the benchmarks using the 4.0 implementation with LVM configured as a baseline. DRAM results are also present for comparison. Note that the following throughput numbers are determined by choosing a typical point from the throughput report by eye. Therefore, these numbers can be used only for relative comparison not as exact reports. Since the results are mostly different, it is good enough to reason about the gain with 4.1. 

| Entry size |   DRAM   | PMEM 4.0 | 4.1 Round-robin | 4.1 NUMA-aware |
|-----------:|---------:|---------:|----------------:|---------------:|
|       128B |  1.7Mops |  1.7Mops |         1.7Mops |        1.8Mops |
|        1KB |  1.5Mops |  1.4Mops |         1.4Mops |        1.5Mops |
|        4KB |  800Kops |  300Kops |         470Kops |        800Kops |
|       10KB |  350Kops |  100Kops |         175Kops |        350Kops | 
|      100KB |   35Kops |   10Kops |          12Kops |         34Kops | 

What we can conclude from the table is that with smaller entry sizes the DRAM and the PMEM results are head-to-head, both with 4.0 and 4.1 implementation. With bigger entry sizes, such as 4KB the difference is significant. There is a big gain even with the round-robin allocation strategy. With the NUMA-aware strategy the PMEM throughput is lifted up to the same level where DRAM throughput is. 

More results to be added later.

#### Future Improvement Ideas

##### Remove the libpmem Dependency

We use `libpmem` only to tell if a directory is on persistent memory or not. Adding a shared library into our jar only for this check can be seen as unnecessary, and we can implement this logic ourselves reducing the jar size. 

##### Hazelcast PMEM Allocator

Memkind uses jemalloc under the hood, which comes with its own known fragmentation. If we guarantee that we use PMEM backed allocations only from our POOLED memory manager that never uncommits or reallocates memory, the structures that Memkind and jemalloc maintain is unnecessary, and we can potentially implement our allocator that allocates from thread-local buffers with pointer bumping similarly to how the HotSpot JVM allocates in its eden region. This could reduce the fragmentation considerably. This would not remove the potentially high internal fragmentation caused by our buddy allocator.

##### NUMA-aware Allocation Overflowing

The allocation overflowing follows the same very simple algorithm for both allocation strategies: iterates through all PMEM heaps in a round-robin manner. This ignores the fact that not all NUMA-remote accesses cost the same. In 4-socket machines with 2 UPI links and 8-socket machines in general there are NUMA nodes that are directly connected and others that require one more additional hop to communicate with. This fact is encoded into the NUMA distance abstract value that we can query with `libnuma`. Therefore, we can change the allocation overflowing algorithm for the NUMA-aware strategy to overflow to the other heaps based on their NUMA distance. 

#### Other Artifacts
* Blog post about configuring Optane for KMEM DAX mode: [How To Extend Volatile System Memory (RAM) using Persistent Memory on
 Linux](https://stevescargall.com/2019/07/09/how-to-extend-volatile-system-memory-ram-using-persistent-memory-on-linux/)
* [Memkind Architecture (pdf)](http://memkind.github.io/memkind/memkind_arch_20150318.pdf)
* [Memkind API](http://memkind.github.io/memkind/man_pages/memkind.html)  
* Memkind tutorial for PMEM kind: [Use Memkind to Manage Large Volatile Memory Capacity on Intel® Optane™ DC Persistent Memory](https://software.intel.com/en-us/articles/use-memkind-to-manage-volatile-memory-on-intel-optane-persistent-memory)
* Blog post about configuring and using KMEM DAX mode with Memkind: [Memkind support for KMEM DAX option](https://pmem.io/2020/01/20/memkind-dax-kmem.html) 
* [Intel Persistent Memory FAQ](https://software.intel.com/en-us/articles/persistent-memory-faq)
* [Man page of /proc with sections discussing calculation of the OOM score](http://man7.org/linux/man-pages/man5/proc.5.html)
* [Man page of fallocate](https://www.man7.org/linux/man-pages/man2/fallocate.2.html) (hole punching, search for
 `FALLOC_FL_PUNCH_HOLE`)
* Blog post from Pivotal on overcommitting: [Virtual memory settings in Linux - The Problem with Overcommit](http://engineering.pivotal.io/post/virtual_memory_settings_in_linux_-_the_problem_with_overcommit/)
