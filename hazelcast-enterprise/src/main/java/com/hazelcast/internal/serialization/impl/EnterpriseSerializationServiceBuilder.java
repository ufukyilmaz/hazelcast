package com.hazelcast.internal.serialization.impl;

import com.hazelcast.config.SerializationConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ManagedContext;
import com.hazelcast.core.PartitioningStrategy;
import com.hazelcast.internal.serialization.InputOutputFactory;
import com.hazelcast.internal.serialization.SerializationServiceBuilder;
import com.hazelcast.internal.serialization.impl.bufferpool.BufferPoolFactory;
import com.hazelcast.memory.MemoryManager;
import com.hazelcast.nio.UnsafeHelper;
import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.nio.serialization.PortableFactory;

import java.nio.ByteOrder;

public class EnterpriseSerializationServiceBuilder extends DefaultSerializationServiceBuilder
        implements SerializationServiceBuilder {

    private MemoryManager memoryManager;
    private BufferPoolFactory bufferPoolFactory = new EnterpriseBufferPoolFactory();

    public EnterpriseSerializationServiceBuilder setMemoryManager(MemoryManager memoryManager) {
        this.memoryManager = memoryManager;
        return this;
    }

    @Override
    public EnterpriseSerializationServiceBuilder setVersion(byte version) {
        return (EnterpriseSerializationServiceBuilder) super.setVersion(version);
    }

    @Override
    public EnterpriseSerializationServiceBuilder setPortableVersion(int portableVersion) {
        return (EnterpriseSerializationServiceBuilder) super.setPortableVersion(portableVersion);
    }

    @Override
    public EnterpriseSerializationServiceBuilder setClassLoader(ClassLoader classLoader) {
        return (EnterpriseSerializationServiceBuilder) super.setClassLoader(classLoader);
    }

    @Override
    public EnterpriseSerializationServiceBuilder setConfig(SerializationConfig config) {
        return (EnterpriseSerializationServiceBuilder) super.setConfig(config);
    }

    @Override
    public EnterpriseSerializationServiceBuilder addDataSerializableFactory(int id, DataSerializableFactory factory) {
        return (EnterpriseSerializationServiceBuilder) super.addDataSerializableFactory(id, factory);
    }

    @Override
    public EnterpriseSerializationServiceBuilder addPortableFactory(int id, PortableFactory factory) {
        return (EnterpriseSerializationServiceBuilder) super.addPortableFactory(id, factory);
    }

    @Override
    public EnterpriseSerializationServiceBuilder addClassDefinition(ClassDefinition cd) {
        return (EnterpriseSerializationServiceBuilder) super.addClassDefinition(cd);
    }

    @Override
    public EnterpriseSerializationServiceBuilder setCheckClassDefErrors(boolean checkClassDefErrors) {
        return (EnterpriseSerializationServiceBuilder) super.setCheckClassDefErrors(checkClassDefErrors);
    }

    @Override
    public EnterpriseSerializationServiceBuilder setManagedContext(ManagedContext managedContext) {
        return (EnterpriseSerializationServiceBuilder) super.setManagedContext(managedContext);
    }

    @Override
    public EnterpriseSerializationServiceBuilder setUseNativeByteOrder(boolean useNativeByteOrder) {
        return (EnterpriseSerializationServiceBuilder) super.setUseNativeByteOrder(useNativeByteOrder);
    }

    @Override
    public EnterpriseSerializationServiceBuilder setByteOrder(ByteOrder byteOrder) {
        return (EnterpriseSerializationServiceBuilder) super.setByteOrder(byteOrder);
    }

    @Override
    public EnterpriseSerializationServiceBuilder setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        return (EnterpriseSerializationServiceBuilder) super.setHazelcastInstance(hazelcastInstance);
    }

    @Override
    public EnterpriseSerializationServiceBuilder setEnableCompression(boolean enableCompression) {
        return (EnterpriseSerializationServiceBuilder) super.setEnableCompression(enableCompression);
    }

    @Override
    public EnterpriseSerializationServiceBuilder setEnableSharedObject(boolean enableSharedObject) {
        return (EnterpriseSerializationServiceBuilder) super.setEnableSharedObject(enableSharedObject);
    }

    @Override
    public EnterpriseSerializationServiceBuilder setAllowUnsafe(boolean allowUnsafe) {
        return (EnterpriseSerializationServiceBuilder) super.setAllowUnsafe(allowUnsafe);
    }

    @Override
    public EnterpriseSerializationServiceBuilder setPartitioningStrategy(PartitioningStrategy partitionStrategy) {
        return (EnterpriseSerializationServiceBuilder) super.setPartitioningStrategy(partitionStrategy);
    }

    @Override
    public EnterpriseSerializationServiceBuilder setInitialOutputBufferSize(int initialOutputBufferSize) {
        return (EnterpriseSerializationServiceBuilder) super.setInitialOutputBufferSize(initialOutputBufferSize);
    }

    @Override
    public EnterpriseSerializationService build() {
        return (EnterpriseSerializationService) super.build();
    }

    @Override
    protected EnterpriseSerializationServiceV1 createSerializationService(InputOutputFactory inputOutputFactory) {
        switch (version) {
            case 1:
                EnterpriseSerializationServiceV1 serializationServiceV1 = new EnterpriseSerializationServiceV1(inputOutputFactory,
                        version, portableVersion, classLoader, dataSerializableFactories, portableFactories, managedContext,
                        partitioningStrategy, initialOutputBufferSize, bufferPoolFactory, memoryManager);
                serializationServiceV1.registerJavaTypeSerializers(enableCompression, enableSharedObject);
                serializationServiceV1.registerClassDefinitions(classDefinitions, checkClassDefErrors);
                return serializationServiceV1;

            //Future version note: add new versions here
            //adding case's for each version and instantiate it properly
            default:
                throw new IllegalArgumentException("Serialization version is not supported!");
        }
    }

    protected InputOutputFactory createInputOutputFactory() {
        if (byteOrder == null) {
            byteOrder = ByteOrder.BIG_ENDIAN;
        }
        if (useNativeByteOrder || byteOrder == ByteOrder.nativeOrder()) {
            byteOrder = ByteOrder.nativeOrder();
            if (allowUnsafe && UnsafeHelper.UNSAFE_AVAILABLE) {
                return new EnterpriseUnsafeInputOutputFactory();
            }
        }
        return new EnterpriseByteArrayInputOutputFactory(byteOrder);
    }
}
