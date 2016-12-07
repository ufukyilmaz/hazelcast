package com.hazelcast.client.map.impl.proxy;

import com.hazelcast.client.proxy.ClientMapProxy;
import com.hazelcast.core.IEnterpriseMap;

/**
 * Contains enterprise-part extensions to {@code ClientMapProxy}.
 * Since 3.8, continuous query cache has been moved to open source, so this class is an empty shell.
 */
public class EnterpriseClientMapProxyImpl extends ClientMapProxy implements IEnterpriseMap {

    EnterpriseClientMapProxyImpl(String serviceName, String name) {
        super(serviceName, name);
    }

}
