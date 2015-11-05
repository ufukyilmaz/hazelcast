package com.hazelcast.spi.hotrestart;

import java.util.Collection;

/**
 * Specifies operations that the Hot Restart store will request from the
 * RAM store. A RAM store is an abstraction over the regular Hazelcast in-memory
 * store of the data which is additionally managed by a Hot Restart store.
 */
public interface RamStore {
    /**
     * Copies the key/value bytes into the corresponding byte buffers provided
     * by the supplied {@link RecordDataSink}. Before copying checks that the actual
     * size of the record matches the value of the {@code expectedSize} parameter.
     * If it doesn't match, the method returns {@code false}.
     * <p>
     * If the requested record is a tombstone, only the key bytes will be written.
     * <p>
     * If this method returns true, then the data stored from 0 to <code>position</code>
     * of the key and value buffers represents the key and value data of the record.
     * <p>
     * If this method returns false, the state of the byte buffers is unspecified.
     * <p>
     * The caller must ensure that the buffers returned from {@code RecordDataSink}
     * initially have {@code position == 0}.
     * @param expectedSize the expected size of the record (key + value).
     * @throws HotRestartException if a record identified by the supplied key handle was not found
     */
    boolean copyEntry(KeyHandle key, int expectedSize, RecordDataSink bufs) throws HotRestartException;

    /**
     * The data of a record in the Hot Restart store is not kept
     * by the Hot Restart store and the same holds even for tombstone
     * records, representing entries which were deleted from the RAM store.
     * The RAM store must retain a key even after deletion because the Hot
     * Restart store will ask for it when garbage-collecting a chunk file
     * which contains the tombstone record.
     * <p>
     * Eventually the Hot Restart store will conclude it is safe to remove
     * the tombstone from the persistent store; at that point it will notify
     * the RAM store that it can remove the key as well. It will do so by
     * calling this method.
     */
    void releaseTombstones(Collection<TombstoneId> handles);

    /**
     * Called during Hot Restart. Requests a handle object for the given
     * key bytes.
     *
     * @return the key handle identifying the supplied key bytes.
     */
    KeyHandle toKeyHandle(byte[] key);

    /**
     * Called during Hot Restart. Allows the RAM store to re-establish a
     * mapping from the supplied key to the supplied value.
     */
    void accept(KeyHandle hrKey, byte[] value);

    /**
     * Called during Hot Restart. Allows the RAM store to re-establish a
     * mapping from the supplied key to a tombstone with the supplied
     * sequence ID.
     */
    void acceptTombstone(KeyHandle hrKey, long seq);

    /**
     * Holder of the pair (keyHandle, tombstoneSeq) as required by the
     * tombstone releasing mechanism, as explained in {@link RamStore#releaseTombstones(Collection)}.
     * <p>
     * This mechanism is subject to the A-B-A problem where first a mapping for a key
     * is deleted, creating tombstone 1; then a new mapping established, then the new mapping
     * again deleted, creating tombstone 2 with the same key. Without the help of the `tombstoneSeq`
     * field these two tombstones couldn't be distinguished and a {@code releaseTombstones()} call
     * could clear out the wrong tombstone.
     */
    interface TombstoneId {
        /**
         * @return the key handle of the tombstone
         */
        KeyHandle keyHandle();

        /**
         * @return an integer unique to this particular tombstone
         */
        long tombstoneSeq();
    }
}
