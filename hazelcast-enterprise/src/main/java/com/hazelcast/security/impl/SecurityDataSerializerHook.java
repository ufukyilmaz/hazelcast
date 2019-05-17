package com.hazelcast.security.impl;

import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.ArrayDataSerializableFactory;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.security.ClusterEndpointPrincipal;
import com.hazelcast.security.ClusterIdentityPrincipal;
import com.hazelcast.security.ClusterRolePrincipal;
import com.hazelcast.security.SecureCallableImpl;
import com.hazelcast.util.ConstructorFunction;

import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.ENTERPRISE_SECURITY_DS_FACTORY;
import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.ENTERPRISE_SECURITY_DS_FACTORY_ID;

/**
 * DataSerializerHook for com.hazelcast.security classes.
 */
public class SecurityDataSerializerHook implements DataSerializerHook {

    public static final int F_ID = FactoryIdHelper.getFactoryId(ENTERPRISE_SECURITY_DS_FACTORY,
            ENTERPRISE_SECURITY_DS_FACTORY_ID);

    public static final int SECURE_CALLABLE = 0;
    public static final int IDENTITY_PRINCIPAL = 1;
    public static final int ROLE_PRINCIPAL = 2;
    public static final int ENDPOINT_PRINCIPAL = 3;

    private static final int LEN = ENDPOINT_PRINCIPAL + 1;

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public DataSerializableFactory createFactory() {
        ConstructorFunction<Integer, IdentifiedDataSerializable>[] constructors = new ConstructorFunction[LEN];

        constructors[SECURE_CALLABLE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new SecureCallableImpl();
            }
        };
        constructors[IDENTITY_PRINCIPAL] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new ClusterIdentityPrincipal();
            }
        };
        constructors[ROLE_PRINCIPAL] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new ClusterRolePrincipal();
            }
        };
        constructors[ENDPOINT_PRINCIPAL] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new ClusterEndpointPrincipal();
            }
        };

        return new ArrayDataSerializableFactory(constructors);
    }
}
