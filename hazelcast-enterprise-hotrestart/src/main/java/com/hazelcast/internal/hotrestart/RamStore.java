package com.hazelcast.internal.hotrestart;

import com.hazelcast.hotrestart.HotRestartException;
import com.hazelcast.internal.hotrestart.impl.SetOfKeyHandle;

/**
 * Specifies operations that the Hot Restart store will
 * request from the RAM store. A RAM store is an abstraction
 * over the regular Hazelcast in-memory store of the data
 * which is additionally managed by a Hot Restart store.
 */
public interface RamStore {
    /**
     * Copies the key/value bytes into the corresponding byte
     * buffers provided by the supplied {@link RecordDataSink}.
     * Before copying checks that the actual size of the record
     * matches the value of the {@code expectedSize} parameter.
     * If it doesn't match, the method returns {@code false}.
     * <p>
     * If this method returns true, then the data stored from
     * 0 to {@code position} of the key and value buffers
     * represents the key and value data of the record.
     * <p>
     * If this method returns false, the state
     * of the byte buffers is unspecified.
     * <p>
     * The caller must ensure that the buffers returned from {@code
     * RecordDataSink} initially have {@code position == 0}.
     * <p>
     * This method must not acquire any locks which may be held
     * while calling any of the {@link HotRestartStore}'s methods.
     *
     * @param expectedSize the expected
     *                     size of the record (key + value).
     * @throws HotRestartException if a record identified
     *                             by the supplied key handle was not found
     */
    boolean copyEntry(KeyHandle kh, int expectedSize, RecordDataSink bufs) throws HotRestartException;

    /**
     * Called during Hot Restart. Requests a
     * handle object for the given key bytes.
     *
     * @return the key handle identifying the supplied key bytes.
     */
    KeyHandle toKeyHandle(byte[] key);

    /**
     * Called during Hot Restart. Allows the RAM store to re-establish
     * a mapping from the supplied key to the supplied value.
     */
    void accept(KeyHandle kh, byte[] value);

    /**
     * Called during Hot Restart. Gives the RAM store a set of key
     * handles which can be removed because they have null-values.
     */
    void removeNullEntries(SetOfKeyHandle keyHandles);
}
