package com.hazelcast.internal.serialization.impl;


/**
 * The serializaion header consists of one java byte primitive value.
 * Bits are used in the following way (from the least significant to the most significant)
 * 0.) 0=data_serializable, 1=identified_data_serializable
 * 1.) 0=non-versioned, 1=versioned
 * 2.) 0=uncompressed, 1=compressed (either classId and factoryId or class package name)
 * 3.) unused
 * 4.) unused
 * 5.) unused
 * 6.) unused
 * 7.) unused
 * <p>
 * Earlier the header was just a byte holding boolean value:
 * - 0=data_serializable, 1=identified_data_serializable
 * thus the new format is fully backward compatible.
 */
final class EnterpriseDataSerializableHeader {

    private static final byte IDENTIFIED_DATA_SERIALIZABLE = 1 << 0;
    private static final byte VERSIONED = 1 << 1;
    private static final byte COMPRESSED = 1 << 2;

    private EnterpriseDataSerializableHeader() {
    }

    static boolean isIdentifiedDataSerializable(byte header) {
        return (header & IDENTIFIED_DATA_SERIALIZABLE) != 0;
    }

    static boolean isVersioned(byte header) {
        return (header & VERSIONED) != 0;
    }

    static boolean isCompressed(byte header) {
        return (header & COMPRESSED) != 0;
    }

    static byte createHeader(boolean identified, boolean versioned, boolean compressed) {
        byte header = 0;

        if (identified) {
            header |= IDENTIFIED_DATA_SERIALIZABLE;
        }
        if (versioned) {
            header |= VERSIONED;
        }
        if (compressed) {
            header |= COMPRESSED;
        }

        return header;
    }

}
