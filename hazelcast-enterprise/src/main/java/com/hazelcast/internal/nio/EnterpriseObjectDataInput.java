package com.hazelcast.internal.nio;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.internal.serialization.DataType;
import com.hazelcast.internal.serialization.EnterpriseSerializationService;

import java.io.IOException;

/**
 * Contract point for {@link com.hazelcast.nio.ObjectDataInput} implementations on enterprise.
 *
 * @see com.hazelcast.nio.ObjectDataInput
 */
public interface EnterpriseObjectDataInput extends ObjectDataInput {

    /**
     * Reads {@link Data}
     * as given {@link DataType}.
     *
     * @param type the type of the {@link Data} to be read
     * @return the read {@link Data}
     * @throws IOException
     */
    Data readData(DataType type) throws IOException;

    /**
     * Tries to read {@link Data}
     * as given {@link DataType}.
     * If it fails for some reason (such as OOME for native memory data)
     * reads the data to heap.
     *
     * @param type the type of the {@link Data} to be read
     * @return the read {@link Data}
     * @throws IOException
     */
    Data tryReadData(DataType type) throws IOException;

    /**
     * Gets the underlying {@link EnterpriseSerializationService}.
     *
     * @return the underlying {@link EnterpriseSerializationService}.
     */
    EnterpriseSerializationService getSerializationService();

}
