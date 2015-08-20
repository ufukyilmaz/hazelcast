package com.hazelcast.nio;

import com.hazelcast.nio.serialization.EnterpriseSerializationService;

/**
 * Contract point for {@link com.hazelcast.nio.ObjectDataOutput} implementations on enterprise.
 *
 * @see com.hazelcast.nio.ObjectDataInput
 */
public interface EnterpriseObjectDataOutput extends ObjectDataOutput {

    /**
     * Gets the underlying {@link com.hazelcast.nio.serialization.EnterpriseSerializationService}.
     *
     * @return the underlying {@link com.hazelcast.nio.serialization.EnterpriseSerializationService}.
     */
    EnterpriseSerializationService getSerializationService();

}
