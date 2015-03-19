package com.hazelcast.nio;

import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataType;

import java.io.IOException;

/**
 * Provides serialization methods for arrays of primitive types
 */
public interface EnterpriseObjectDataInput extends ObjectDataInput {

    Data readData(DataType type) throws IOException;
    Data tryReadData(DataType type) throws IOException;

}
