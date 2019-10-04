package com.hazelcast.enterprise.wan.impl.discovery;

import com.hazelcast.logging.ILogger;
import com.hazelcast.cluster.Address;
import com.hazelcast.spi.discovery.AbstractDiscoveryStrategy;
import com.hazelcast.spi.discovery.DiscoveryNode;
import com.hazelcast.spi.discovery.SimpleDiscoveryNode;
import com.hazelcast.internal.util.AddressUtil;
import com.hazelcast.internal.util.AddressUtil.AddressHolder;
import com.hazelcast.internal.util.StringUtil;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Map;

/**
 * Discovery strategy implementation which always returns the same list of predefined {@link DiscoveryNode}s. The node
 * addresses are defined as a comma-separated list of IP addresses. The port can be a part of the address or you can
 * define a default and common port using a separate property.
 */
public class StaticDiscoveryStrategy extends AbstractDiscoveryStrategy {
    public StaticDiscoveryStrategy(ILogger logger, Map<String, Comparable> properties) {
        super(logger, properties);
    }

    @Override
    public Iterable<DiscoveryNode> discoverNodes() {
        final String endpoints = getOrDefault(StaticDiscoveryProperties.ENDPOINTS, "");
        final Integer port = getOrDefault(StaticDiscoveryProperties.PORT, 5432);
        final ArrayList<DiscoveryNode> ret = new ArrayList<DiscoveryNode>();

        for (String endpoint : endpoints.split(",")) {
            try {
                final String trimmedEndpoint = endpoint.trim();
                if (!StringUtil.isNullOrEmpty(trimmedEndpoint)) {
                    final AddressHolder holder = AddressUtil.getAddressHolder(trimmedEndpoint, port);
                    final Address addr = new Address(holder.getAddress(), holder.getPort());
                    ret.add(new SimpleDiscoveryNode(addr, addr));
                }
            } catch (UnknownHostException e) {
                throw new RuntimeException("Failed to parse publisher endpoints property", e);
            }
        }
        return ret;
    }
}
