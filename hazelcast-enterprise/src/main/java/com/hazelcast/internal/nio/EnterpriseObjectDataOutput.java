package com.hazelcast.internal.nio;

import com.hazelcast.internal.serialization.EnterpriseSerializationService;
import com.hazelcast.nio.ObjectDataOutput;

/**
 * Contract point for {@link com.hazelcast.nio.ObjectDataOutput} implementations on enterprise.
 *
 * @see com.hazelcast.nio.ObjectDataInput
 */
public interface EnterpriseObjectDataOutput extends ObjectDataOutput {

    /**
     * Gets the underlying {@link EnterpriseSerializationService}.
     *
     * @return the underlying {@link EnterpriseSerializationService}.
     */
    EnterpriseSerializationService getSerializationService();

}
