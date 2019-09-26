package com.hazelcast.internal.serialization.impl;

import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.nio.ClassLoaderUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.nio.serialization.StreamSerializer;
import com.hazelcast.nio.serialization.TypedDataSerializable;
import com.hazelcast.nio.serialization.TypedStreamDeserializer;
import com.hazelcast.nio.serialization.impl.Versioned;
import com.hazelcast.nio.serialization.impl.VersionedDataSerializableFactory;
import com.hazelcast.internal.util.ServiceLoader;
import com.hazelcast.internal.util.collection.Int2ObjectHashMap;
import com.hazelcast.version.Version;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import static com.hazelcast.internal.serialization.impl.EnterpriseDataSerializableHeader.createHeader;
import static com.hazelcast.internal.serialization.impl.EnterpriseDataSerializableHeader.isIdentifiedDataSerializable;
import static com.hazelcast.internal.serialization.impl.EnterpriseDataSerializableHeader.isVersioned;
import static com.hazelcast.internal.serialization.impl.SerializationConstants.CONSTANT_TYPE_DATA_SERIALIZABLE;
import static com.hazelcast.logging.Logger.getLogger;
import static com.hazelcast.internal.util.ExceptionUtil.rethrow;

/**
 * The {@link StreamSerializer} that handles:
 * <ol>
 * <li>{@link DataSerializable}</li>
 * <li>{@link IdentifiedDataSerializable}</li>
 * </ol>
 * <p>
 * This is the enterprise version of the DataSerializableSerializer that handles the versioning.
 * Each object annotated by the @Versioned annotation will get the cluster major.minor version included in the byte stream
 * if the cluster version is not set to UNKNOWN.
 * It is only used if rolling-upgrades are enabled (for backward compatibility).
 * <p>
 * We don't send the patch version since there it is impossible to put patch version releases in a time ordered sequence.
 * 3.8.5 might have been released after 3.9.2. In order to know that we would have to hard-code the release order
 * into the code. What we can order are the minor releases -> it's obvious that 3.8 was released before 3.9.
 * <p>
 * What is more, there should not be any byte format differences between patch releases.
 * Between two patch releases everything should work out-of-the-box due to our binary compatibility policy and we don't
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
            Iterator<DataSerializerHook> hooks = ServiceLoader.iterator(DataSerializerHook.class, FACTORY_ID, classLoader);
            while (hooks.hasNext()) {
                DataSerializerHook hook = hooks.next();
                DataSerializableFactory factory = hook.createFactory();
                if (factory != null) {
                    register(hook.getFactoryId(), factory);
                }
            }
        } catch (Exception e) {
            throw rethrow(e);
        }

        if (dataSerializableFactories != null) {
            for (Map.Entry<Integer, ? extends DataSerializableFactory> entry : dataSerializableFactories.entrySet()) {
                register(entry.getKey(), entry.getValue());
            }
        }
    }

    private void register(int factoryId, DataSerializableFactory factory) {
        DataSerializableFactory current = factories.get(factoryId);
        if (current != null) {
            if (current.equals(factory)) {
                getLogger(getClass()).warning("DataSerializableFactory[" + factoryId + "] is already registered! Skipping "
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
    public void destroy() {
        factories.clear();
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
            factoryId = in.readInt();
            classId = in.readInt();

            // if versions were previously set while processing an outer object,
            // keep the current values and restore them in the end
            Version previousClusterVersion = in.getVersion();
            Version previousWanProtocolVersion = in.getWanProtocolVersion();

            if (isVersioned(header)) {
                readVersions(in);
            } else {
                in.setVersion(Version.UNKNOWN);
                in.setWanProtocolVersion(Version.UNKNOWN);
            }

            // populate the object
            DataSerializable ds = instance != null
                    ? instance
                    : createIdentifiedDataSerializable(in.getVersion(), in.getWanProtocolVersion(), factoryId, classId);
            ds.readData(in);

            // restore the original versions
            in.setVersion(previousClusterVersion);
            in.setWanProtocolVersion(previousWanProtocolVersion);
            return ds;
        } catch (Exception ex) {
            throw rethrowIdsReadException(factoryId, classId, ex);
        }
    }

    /**
     * Reads the cluster and WAN protocol version from the serialised stream and
     * sets them on the {@code in} object.
     *
     * @param in the input stream containing the cluster or WAN protocol version
     * @throws IOException if an I/O error occurs.
     */
    private void readVersions(ObjectDataInput in) throws IOException {
        byte major = in.readByte();
        byte minor = in.readByte();
        assert clusterVersionAware.getClusterVersion() != null;
        if (major < 0) {
            in.setWanProtocolVersion(Version.of(-major, minor));
        } else {
            in.setVersion(Version.of(major, minor));
        }
    }

    private DataSerializable createIdentifiedDataSerializable(Version clusterVersion,
                                                              Version wanProtocolVersion,
                                                              int factoryId,
                                                              int classId) {
        DataSerializableFactory dsf = factories.get(factoryId);
        if (dsf == null) {
            throw new HazelcastSerializationException("No DataSerializerFactory registered for namespace: " + factoryId);
        }
        if (dsf instanceof VersionedDataSerializableFactory) {
            return ((VersionedDataSerializableFactory) dsf).create(classId, clusterVersion, wanProtocolVersion);
        } else {
            return dsf.create(classId);
        }
    }

    private DataSerializable readDataSerializable(ObjectDataInput in, byte header, DataSerializable instance) throws IOException {
        String className = in.readUTF();
        try {
            // if versions were previously set while processing an outer object,
            // keep the current values and restore them in the end
            Version previousClusterVersion = in.getVersion();
            Version previousWanProtocolVersion = in.getWanProtocolVersion();

            if (isVersioned(header)) {
                readVersions(in);
            } else {
                in.setVersion(Version.UNKNOWN);
                in.setWanProtocolVersion(Version.UNKNOWN);
            }

            DataSerializable ds = instance != null ? instance
                    : ClassLoaderUtil.newInstance(in.getClassLoader(), className);
            ds.readData(in);

            // restore the original versions
            in.setVersion(previousClusterVersion);
            in.setWanProtocolVersion(previousWanProtocolVersion);
            return ds;
        } catch (Exception ex) {
            throw rethrowDsReadException(className, ex);
        }
    }

    @Override
    public void write(ObjectDataOutput out, DataSerializable obj) throws IOException {
        // if versions were previously set while processing an outer object,
        // keep the current values and restore them in the end
        Version previousClusterVersion = out.getVersion();

        if (out.getWanProtocolVersion() == Version.UNKNOWN) {
            // if WAN protocol version has not been yet explicitly set, then
            // check if class is Versioned to set cluster version as output version
            Version version = (obj instanceof Versioned)
                    ? clusterVersionAware.getClusterVersion()
                    : Version.UNKNOWN;
            out.setVersion(version);
        }

        if (obj instanceof IdentifiedDataSerializable) {
            writeIdentifiedDataSerializable(out, (IdentifiedDataSerializable) obj, out.getVersion(), out.getWanProtocolVersion());
        } else {
            writeDataSerializable(out, obj, out.getVersion(), out.getWanProtocolVersion());
        }
        obj.writeData(out);

        // restore the original version
        if (out.getWanProtocolVersion() == Version.UNKNOWN) {
            out.setVersion(previousClusterVersion);
        }
    }

    private void writeIdentifiedDataSerializable(ObjectDataOutput out,
                                                 IdentifiedDataSerializable obj,
                                                 Version clusterVersion,
                                                 Version wanProtocolVersion) throws IOException {

        boolean versioned = clusterVersion != Version.UNKNOWN || wanProtocolVersion != Version.UNKNOWN;

        out.writeByte(createHeader(true, versioned));

        out.writeInt(obj.getFactoryId());
        out.writeInt(obj.getClassId());

        if (wanProtocolVersion != Version.UNKNOWN) {
            // we write out WAN protocol versions as negative major version
            out.writeByte(-wanProtocolVersion.getMajor());
            out.writeByte(wanProtocolVersion.getMinor());
        } else if (clusterVersion != Version.UNKNOWN) {
            out.writeByte(clusterVersion.getMajor());
            out.writeByte(clusterVersion.getMinor());
        }
    }

    private void writeDataSerializable(ObjectDataOutput out,
                                       DataSerializable obj,
                                       Version clusterVersion,
                                       Version wanProtocolVersion) throws IOException {
        boolean versioned = clusterVersion != Version.UNKNOWN || wanProtocolVersion != Version.UNKNOWN;
        out.writeByte(createHeader(false, versioned));

        if (obj instanceof TypedDataSerializable) {
            out.writeUTF(((TypedDataSerializable) obj).getClassType().getName());
        } else {
            out.writeUTF(obj.getClass().getName());
        }

        if (wanProtocolVersion != Version.UNKNOWN) {
            // we write out WAN protocol versions as negative major version
            out.writeByte(-wanProtocolVersion.getMajor());
            out.writeByte(wanProtocolVersion.getMinor());
        } else if (clusterVersion != Version.UNKNOWN) {
            out.writeByte(clusterVersion.getMajor());
            out.writeByte(clusterVersion.getMinor());
        }
    }

    private static IOException rethrowIdsReadException(int factoryId, int classId, Exception e) throws IOException {
        if (e instanceof IOException) {
            throw (IOException) e;
        }
        if (e instanceof HazelcastSerializationException) {
            throw (HazelcastSerializationException) e;
        }
        throw new HazelcastSerializationException("Problem while reading IdentifiedDataSerializable, namespace: " + factoryId
                + ", classId: " + classId
                + ", exception: " + e.getMessage(), e);
    }

    private static IOException rethrowDsReadException(String className, Exception e) throws IOException {
        if (e instanceof IOException) {
            throw (IOException) e;
        }
        if (e instanceof HazelcastSerializationException) {
            throw (HazelcastSerializationException) e;
        }
        throw new HazelcastSerializationException("Problem while reading DataSerializable, class-name: " + className
                + ", exception: " + e.getMessage(), e);
    }
}
