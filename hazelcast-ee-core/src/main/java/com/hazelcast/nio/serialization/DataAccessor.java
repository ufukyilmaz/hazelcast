package com.hazelcast.nio.serialization;

/**
 * @author mdogan 10/7/13
 */
public class DataAccessor {

    public static void setCD(Data data, ClassDefinition cd) {
        data.classDefinition = cd;
    }
}
