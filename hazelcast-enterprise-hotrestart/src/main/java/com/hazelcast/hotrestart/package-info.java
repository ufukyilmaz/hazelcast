/**
 * Public API for the Hot Restart store.
 * <p>A Hot Restart store is used internally by Hazelcast to provide the Hot Restart feature: the ability to restart
 * after a crash or shutdown with its data structures restored to the state before going down.
 * <p>
 * The Hot Restart Store exposes the following probes: <ul>
 *     <li>{@code hot-restart.<store_name>.valOccupancy}: approximate amount of data stored in main chunk files
 *     (does not account for data in the active chunk);</li>
 *     <li>{@code hot-restart.<store_name>.valGarbage}: approximate amount of garbage within that data
 *     (does not account for garbage in the active chunk);</li>
 *     <li>{@code hot-restart.<store_name>.tombOccupancy}: approximate amount of data stored in tombstone chunk files
 *     (does not account for data in the active tombstone chunk);</li>
 *     <li>{@code hot-restart.<store_name>.tombGarbage}: approximate amount of garbage within that data
 *     (does not account for garbage in the active tombstone chunk);</li>
 *     <li>{@code hot-restart.<store_name>.liveValues}: the number of live <i>value</i> records in the store;</li>
 *     <li>{@code hot-restart.<store_name>.liveTombstones}: the number of live <i>tombstone</i> records in the store.
 *     </li>
 * </ul>
 */
package com.hazelcast.hotrestart;
