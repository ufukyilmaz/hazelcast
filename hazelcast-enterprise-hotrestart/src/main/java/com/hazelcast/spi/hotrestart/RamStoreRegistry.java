package com.hazelcast.spi.hotrestart;

/**
 * RAM store registry for the data managed by a Hot Restart store.
 */
public interface RamStoreRegistry {

    /**
     * Returns the RAM store in charge of the supplied key prefix. Called
     * during normal operation to read data from the store and to release
     * tombstone records.
     * <p>
     * This method is allowed to return {@code null}, in which case the GC
     * process will assume that the RAM store in question has been {@code destroy}ed,
     * but the associated event has not yet reached the GC thread. The GC
     * thread will in that case enter a loop which waits for the said event.
     * If the event never arrives, the loop will be infinite.
     */
    RamStore ramStoreForPrefix(long prefix);

    /**
     * Returns the RAM store in charge of the supplied key prefix.
     * Called by the Hot Restart store during hot restart in order to supply data to it.
     * <p>
     * This method is allowed to return {@code null}, in which case the
     * restarting procedure will assume that the RAM store in question had been
     * destroyed before shutdown.
     */
    RamStore restartingRamStoreForPrefix(long prefix);
}
