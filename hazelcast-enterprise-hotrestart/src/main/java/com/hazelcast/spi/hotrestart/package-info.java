/**
 * SPI for the Hot Restart store.
 * <p>A Hot Restart store is used internally by Hazelcast to provide the Hot Restart feature: the ability to restart
 * after a crash or shutdown with its data structures restored to the state before going down.
 * <p>
 * The Hot Restart Store exposes the following probes: <ul>
 *     <li>{@code hot-restart.<store_name>.occupancy}: approximate amount of data stored in chunk files
 *     (does not account for data in the active chunk);</li>
 *     <li>{@code hot-restart.<store_name>.garbage}: approximate amount of garbage within that data
 *     (does not account for garbage in the active chunk);</li>
 *     <li>{@code hot-restart.<store_name>.liveValues}: the number of live "normal" records in the store;</li>
 *     <li>{@code hot-restart.<store_name>.liveTombstones}: the number of live tombstone records in the store.</li>
 * </ul>
 */
package com.hazelcast.spi.hotrestart;
