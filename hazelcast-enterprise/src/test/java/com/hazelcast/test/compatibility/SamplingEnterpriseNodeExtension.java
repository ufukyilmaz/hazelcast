package com.hazelcast.test.compatibility;

import com.hazelcast.instance.EnterpriseNodeExtension;
import com.hazelcast.instance.Node;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;

/**
 * Used by reflection in MockNodeContext, for properly setting up the serialization service to sample
 * objects serialized during test suite execution.
 */
@SuppressWarnings("unused")
public class SamplingEnterpriseNodeExtension extends EnterpriseNodeExtension {

    public SamplingEnterpriseNodeExtension(Node node) {
        super(node);
    }

    @Override
    public InternalSerializationService createSerializationService() {
        EnterpriseSerializationService serializationService =
                (EnterpriseSerializationService) super.createSerializationService();
        return new SamplingEnterpriseSerializationService(serializationService);
    }
}
