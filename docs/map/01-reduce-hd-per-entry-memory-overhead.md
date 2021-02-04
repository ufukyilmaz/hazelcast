# Reduce HD Per Entry Memory Overhead

|ℹ️ Since: 4.2|
|-------------|

## Rationale:

### Importance of  1 Byte

To get rid of GC effect, we use HD memory. HD memory is out of jvm’s
regular heap space. It directly allocates memory. By default,
to allocate and to free memory effectively, each thread has its own
memory space. This memory space is managed with buddy memory allocation
algorithm. Buddy allocates memory power-of-2 sizes. 4-8-16… bytes. It is
a proven algorithm which is simple and good trade-offs in terms of
fragmentation. In our implementation, if you request N bytes allocation,
internally, buddy allocator adds 1 byte internal overhead to it and
tries to allocate N+1 bytes. If you have 63 bytes it will fit in 64
bytes block, but if you have 64 bytes, it needs 128 bytes Even 1 byte is
important.

## Background

IMap HD record wraps the value put with its metadata. But this metadata
is not required for all use cases, it is mostly feature/stats specific.
Having it for all entries, consumes HD memory unneededly.

Current representation of simple HD Record with metadata:

Metadata Field Name            |   Size  |
--- | --- 
Value Address            |   8 bytes (long)   |
Version                  |   8 bytes (long)   |
Creation Time            |   4 bytes (int)    |
Time To live             |   4 bytes (int)    |
Max Idle                 |   4 bytes (int)    |
Last Access Time         |   4 bytes (int)    |
Last Update Time         |   4 bytes (int)    |
Hits                     |   4 bytes (int)    |
Last Stored Time         |   4 bytes (int)    |
Expiration Time          |   4 bytes (int)    |
Sequence                 |   4 bytes (int)    |
|________________|
|Total size = 52 bytes |

__NOTE__: In current buddy memory allocator design, this record is going
to use a 64 bytes memory block.

### Goal

To have the metadata when needed.

## Design

#### Pros

- Records will be created based on map configuration. If config doesn't
  mandate usage of a specific metadata, that metadata will not be a part
  of HD record. Record creation per map-config will be introduced.

#### Cons

- Needs refactoring to isolate some of existing systems.

### Implementation

## What Do We Have? For What Is It Used?

These are corresponding features for each metadata field:

Field        |       Where is it used|
--- | --- 
value-address      | holds value reference| 
hits               | stats, LFU |
last-access-time   | stats, LRU, max-idle-expiry |
last-update-time   | stats, ttl-expiry |
sequence           | hot-restart |
version            | stats, txn |
creation-time      | stats |
ttl                | stats |
max-idle           | stats |
last-stored-time   | stats |
expiration-time    | stats |

Conclusions from here:

- Regardless of expiration, eviction, stats enabled or disabled, HD
  record has all fields.
- Features and record structure is tightly coupled.
- When stats are enabled not much room for improvement for HD record
  size.

In this design we will focus on improvements when stats are disabled.

## Improvement When Stats are Disabled
First, we need to categorize features to see, for which
ones tight coupling to record structure can be removed.

### Static and Dynamic Features

#### Static Features:

These are not changeable along the lifetime of a record like hot-restart
and eviction, it is ok to hold metadata on record in this case.

- eviction, stats, hot-restart

#### Dynamic Features:

These are added at runtime and can be removed or added multiple times
during the lifetime of a record. Having metadata for dynamic features is
waste of space when there is no need for it.

- Main example for this kind is expiration.

## Isolating Dynamic Features from Record Structure

To support creating-records-per-configuration we need to isolate.

### Isolating Expiration System

Expiration-feature can be added/removed to an entry at __runtime__. To
make it independent of record structure, expiration system will be
refactored and corresponding metadata fields will be removed from
record. Like indexing, we will have a separate system for expiration.
This new system will be running regardless of stats disabled or enabled.

Expiry Metadata:

4 bytes | 4 bytes | 4 bytes | 
--- | --- | --- |
ttl | max-idle | expiration-time |

Bonus:
- Reaching expired entries directly within whole entry set
- Expiration will be faster
- Partition threads will be more free because background expiration task
  will not need to scan whole entry set, to find just one expired entry.

### Stats Enabled (Includes All Static Features)

When stats are enabled, we will have 32 bytes HD record structure.

`HDRecordWithStats:`= 32 bytes

8 bytes | 4 bytes | 4 bytes | 4 bytes | 4 bytes | 4 bytes | 4 bytes | 
--- | --- | --- |--- | --- | --- |--- | 
value-address | hits | last-access-time | last-update-time | sequence | creation-time | last-stored-time |

### Stats Disabled Needs New Record Types Per Config
When stats are disabled, we will have new types of simple HD record
structures per map-config.

#### New HD Record Types

`HDSimpleRecord:`

N bytes |
--- |
Value itself (not address of value)| 

`HDSimpleRecordWithEviction:`= 12 bytes

8 bytes | 4 bytes |  
--- | --- |  
value-address | eviction-metadata |

`HDSimpleRecordWithHotRestart:`= 12 bytes

8 bytes | 4 bytes |  
--- | --- |  
value-address | sequence |

`HDSimpleRecordWithEvictionAndHotRestart:`= 16 bytes

8 bytes | 4 bytes | 4 bytes |  
--- | --- |  --- |  
value-address | eviction-metadata |sequence |

#### Example of Used-HD-Memory-Change by using HDSimpleRecord

```java
//With buddy memory allocator:

// stats off = 610MB 
// stats on  = 1221MB 

        for(int i=0;i< 10_000_000;i++){
        map.set(i,byte8);
        }
```

Bonus:
- Less bytes on network during data replication

## Removing Partition Hash From Value

Key has partition hash but this is not required for `NativeMemoryData`
value.

From 12 bytes --> 8 bytes

4 bytes | 4 bytes | 4 bytes |  
--- | --- | --- |
SIZE_OFFSET | PARTITION_HASH_OFFSET | TYPE_OFFSET |

## Open Issue

- Txn uses `version` field of record to see it is not changed after last
  read. Where should `version` field go?

Current Ideas:

- We can also add it to all HD records as a metadata field.
- Version field can be hold in a separate data structure, this is only
  used for transaction mechanism.
- Maybe we can put it into LockResourceImpl
