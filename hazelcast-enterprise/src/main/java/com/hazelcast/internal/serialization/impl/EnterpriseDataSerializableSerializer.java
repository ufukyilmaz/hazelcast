package com.hazelcast.internal.serialization.impl;

import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.ClassLoaderUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.Version;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.nio.serialization.StreamSerializer;
import com.hazelcast.nio.serialization.TypedStreamDeserializer;
import com.hazelcast.nio.serialization.impl.Versioned;
import com.hazelcast.nio.serialization.impl.VersionedDataSerializableFactory;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.ServiceLoader;
import com.hazelcast.util.collection.Int2ObjectHashMap;
import com.hazelcast.version.ClusterVersion;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import static com.hazelcast.internal.serialization.impl.EnterpriseDataSerializableHeader.createHeader;
import static com.hazelcast.internal.serialization.impl.EnterpriseDataSerializableHeader.isCompressed;
import static com.hazelcast.internal.serialization.impl.EnterpriseDataSerializableHeader.isIdentifiedDataSerializable;
import static com.hazelcast.internal.serialization.impl.EnterpriseDataSerializableHeader.isVersioned;
import static com.hazelcast.internal.serialization.impl.SerializationConstants.CONSTANT_TYPE_DATA_SERIALIZABLE;

/**
 * The {@link StreamSerializer} that handles:
 * <ol>
 * <li>{@link DataSerializable}</li>
 * <li>{@link IdentifiedDataSerializable}</li>
 * </ol>
 * <p>
 * This is the enterprise version of the DataSerializableSerializer that handles the versioning.
 * Each object annotated by the @Versioned annotation will get the cluster minor version included in the byte stream
 * if the cluster version is not set to UNKNOWN.
 * It is only used if rolling-upgrades are enabled (for backward compatibility).
 * <p>
 * Why do we only send the minor version?
 * <p>
 * We don't send the major version since the versioning works only across minor releases.
 * <p>
 * We don't send the patch version since there it is impossible to put patch version releases in a time ordered sequence.
 * 3.8.5 might have been released after 3.9.2. In order to know that we would have to hard-code the release order
 * into the code. What we can order are the minor releases -> it's obvious that 3.8 was released before 3.9.
 * <p>
 * What is more, there should not be any byte format differences between patch releases.
 * Between two patch releases everything should work out of the box due to our binary compatibility policy and we don't
 * want to change that. Enabling binary versioning among patch releases would cause a lot of overhead and would make our
 * coding and testing process massively more complex.
 */
public final class EnterpriseDataSerializableSerializer implements StreamSerializer<DataSerializable>,
        TypedStreamDeserializer<DataSerializable> {

    private static final String FACTORY_ID = "com.hazelcast.DataSerializerHook";

    private final Int2ObjectHashMap<DataSerializableFactory> factories = new Int2ObjectHashMap<DataSerializableFactory>();

    private final EnterpriseClusterVersionAware clusterVersionAware;

    EnterpriseDataSerializableSerializer(Map<Integer, ? extends DataSerializableFactory> dataSerializableFactories,
                                         ClassLoader classLoader, EnterpriseClusterVersionAware clusterVersionAware) {
        this.clusterVersionAware = clusterVersionAware;
        try {
            final Iterator<DataSerializerHook> hooks = ServiceLoader.iterator(DataSerializerHook.class, FACTORY_ID, classLoader);
            while (hooks.hasNext()) {
                DataSerializerHook hook = hooks.next();
                final DataSerializableFactory factory = hook.createFactory();
                if (factory != null) {
                    register(hook.getFactoryId(), factory);
                }
            }
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }

        if (dataSerializableFactories != null) {
            for (Map.Entry<Integer, ? extends DataSerializableFactory> entry : dataSerializableFactories.entrySet()) {
                register(entry.getKey(), entry.getValue());
            }
        }
    }

    private void register(int factoryId, DataSerializableFactory factory) {
        final DataSerializableFactory current = factories.get(factoryId);
        if (current != null) {
            if (current.equals(factory)) {
                Logger.getLogger(getClass()).warning("DataSerializableFactory[" + factoryId + "] is already registered! Skipping "
                        + factory);
            } else {
                throw new IllegalArgumentException("DataSerializableFactory[" + factoryId + "] is already registered! "
                        + current + " -> " + factory);
            }
        } else {
            factories.put(factoryId, factory);
        }
    }

    @Override
    public int getTypeId() {
        return CONSTANT_TYPE_DATA_SERIALIZABLE;
    }

    @Override
    public DataSerializable read(ObjectDataInput in, Class clazz) throws IOException {
        DataSerializable instance = null;
        if (null != clazz) {
            try {
                instance = (DataSerializable) clazz.newInstance();
            } catch (Exception e) {
                throw new HazelcastSerializationException("Requested class " + clazz + " could not be instantiated.", e);
            }
        }

        return doRead(in, instance);
    }

    @Override
    public DataSerializable read(ObjectDataInput in) throws IOException {
        return doRead(in, null);
    }

    private DataSerializable doRead(ObjectDataInput in, DataSerializable instance) throws IOException {
        byte header = in.readByte();
        if (isIdentifiedDataSerializable(header)) {
            return readIdentifiedDataSerializable(in, header, instance);
        } else {
            return readDataSerializable(in, header, instance);
        }
    }

    private DataSerializable readIdentifiedDataSerializable(ObjectDataInput in, byte header, DataSerializable instance)
            throws IOException {
        int factoryId = 0;
        int classId = 0;
        try {
            // read factoryId & classId
            if (isCompressed(header)) {
                factoryId = in.readByte();
                classId = in.readByte();
            } else {
                factoryId = in.readInt();
                classId = in.readInt();
            }

            // read version
            Version version = isVersioned(header) ? readVersion(in) : Version.UNKNOWN;
            setInputVersion(in, version);

            // populate the object
            DataSerializable ds = instance != null ? instance : createIdentifiedDataSerializable(version, factoryId, classId);
            ds.readData(in);

            return ds;
        } catch (Exception ex) {
            rethrowIdsReadException(factoryId, classId, ex);
        }
        return null;
    }

    private Version readVersion(ObjectDataInput in) throws IOException {
        byte minor = in.readByte();
        ClusterVersion v = clusterVersionAware.getClusterVersion();
        assert v != null;
        return Version.of(minor);
    }

    private DataSerializable createIdentifiedDataSerializable(Version version, int factoryId, int classId) {
        DataSerializable ds;
        final DataSerializableFactory dsf = factories.get(factoryId);
        if (dsf == null) {
            throw new HazelcastSerializationException("No DataSerializerFactory registered for namespace: " + factoryId);
        }
        if (dsf instanceof VersionedDataSerializableFactory) {
            ds = ((VersionedDataSerializableFactory) dsf).create(classId, version);
        } else {
            ds = dsf.create(classId);
        }
        return ds;
    }

    private static void rethrowIdsReadException(int factoryId, int classId, Exception e) throws IOException {
        if (e instanceof IOException) {
            throw (IOException) e;
        }
        if (e instanceof HazelcastSerializationException) {
            throw (HazelcastSerializationException) e;
        }
        throw new HazelcastSerializationException("Problem while reading IdentifiedDataSerializable, namespace: "
                + factoryId
                + ", classId: " + classId
                + ", exception: " + e.getMessage(), e);
    }

    private DataSerializable readDataSerializable(ObjectDataInput in, byte header, DataSerializable instance) throws IOException {
        String className = in.readUTF();
        try {
            Version version = isVersioned(header) ? readVersion(in) : Version.UNKNOWN;
            setInputVersion(in, version);

            DataSerializable ds = instance != null ? instance
                    : ClassLoaderUtil.<DataSerializable>newInstance(in.getClassLoader(), className);
            ds.readData(in);

            return ds;
        } catch (Exception ex) {
            rethrowDsReadException(className, ex);
        }
        return null;
    }

    private static IOException rethrowDsReadException(String className, Exception e) throws IOException {
        if (e instanceof IOException) {
            throw (IOException) e;
        }
        if (e instanceof HazelcastSerializationException) {
            throw (HazelcastSerializationException) e;
        }
        throw new HazelcastSerializationException("Problem while reading DataSerializable, class-name: "
                + className
                + ", exception: " + e.getMessage(), e);
    }

    @Override
    public void write(ObjectDataOutput out, DataSerializable obj) throws IOException {
        Version version = (obj instanceof Versioned) ? Version.of(clusterVersionAware.getClusterVersion().getMinor())
                                                        : Version.UNKNOWN;
        setOutputVersion(out, version);

        if (obj instanceof IdentifiedDataSerializable) {
            writeIdentifiedDataSerializable(out, (IdentifiedDataSerializable) obj, version);
        } else {
            writeDataSerializable(out, obj, version);
        }
        obj.writeData(out);
    }

    private void writeIdentifiedDataSerializable(
            ObjectDataOutput out, IdentifiedDataSerializable obj, Version version) throws IOException {

        boolean compressed = areIdsCompressable(obj);
        boolean versioned = version != Version.UNKNOWN;

        out.writeByte(createHeader(true, versioned, compressed));

        if (compressed) {
            out.writeByte(obj.getFactoryId());
            out.writeByte(obj.getId());
        } else {
            out.writeInt(obj.getFactoryId());
            out.writeInt(obj.getId());
        }

        if (versioned) {
            out.writeByte(version.getValue());
        }
    }

    private void writeDataSerializable(ObjectDataOutput out, DataSerializable obj, Version version) throws IOException {
        boolean versioned = version != Version.UNKNOWN;
        out.writeByte(createHeader(false, versioned, false));

        out.writeUTF(obj.getClass().getName());
        if (versioned) {
            out.writeByte(version.getValue());
        }
    }

    private static boolean areIdsCompressable(IdentifiedDataSerializable ids) {
        return isWithinByteRange(ids.getId()) && isWithinByteRange(ids.getFactoryId());
    }

    private static boolean isWithinByteRange(int value) {
        return (byte) value == value;
    }

    private static void setOutputVersion(ObjectDataOutput out, Version version) {
        ((VersionedObjectDataOutput) out).setVersion(version);
    }

    private static void setInputVersion(ObjectDataInput in, Version version) {
        ((VersionedObjectDataInput) in).setVersion(version);
    }

    @Override
    public void destroy() {
        factories.clear();
    }

}
