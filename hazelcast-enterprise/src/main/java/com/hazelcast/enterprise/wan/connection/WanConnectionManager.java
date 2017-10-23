package com.hazelcast.enterprise.wan.connection;

import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.enterprise.wan.EnterpriseWanReplicationService;
import com.hazelcast.instance.Node;
import com.hazelcast.internal.cluster.impl.operations.AuthorizationOp;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.ConnectionManager;
import com.hazelcast.spi.InvocationBuilder;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.discovery.DiscoveryNode;
import com.hazelcast.spi.discovery.impl.PredefinedDiscoveryService;
import com.hazelcast.spi.discovery.integration.DiscoveryService;
import com.hazelcast.util.ConstructorFunction;
import com.hazelcast.wan.WanReplicationPublisher;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.enterprise.wan.replication.WanReplicationProperties.DISCOVERY_PERIOD;
import static com.hazelcast.enterprise.wan.replication.WanReplicationProperties.DISCOVERY_USE_ENDPOINT_PRIVATE_ADDRESS;
import static com.hazelcast.enterprise.wan.replication.WanReplicationProperties.ENDPOINTS;
import static com.hazelcast.enterprise.wan.replication.WanReplicationProperties.GROUP_PASSWORD;
import static com.hazelcast.enterprise.wan.replication.WanReplicationProperties.MAX_ENDPOINTS;
import static com.hazelcast.enterprise.wan.replication.WanReplicationProperties.getProperty;
import static com.hazelcast.util.ConcurrencyUtil.getOrPutSynchronized;
import static com.hazelcast.util.StringUtil.isNullOrEmpty;
import static java.lang.Math.min;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Maintains connections for WAN replication for a single {@link WanReplicationPublisher}.
 */
public class WanConnectionManager {
    /**
     * Default period for running discovery for new endpoints in seconds
     *
     * @see TargetEndpointDiscoveryTask
     */
    private static final int DEFAULT_DISCOVERY_TASK_PERIOD = 10;
    /**
     * Delay in seconds between the initialisation of the connection manager and
     * the time the discovery task is first run.
     *
     * @see TargetEndpointDiscoveryTask
     */
    private static final int DISCOVERY_TASK_START_DELAY = 10;
    /**
     * Default maximum number of endpoints to connect to. This number is
     * used if the user defines the target endpoints using the
     * {@link com.hazelcast.enterprise.wan.replication.WanReplicationProperties#ENDPOINTS}
     * property or if there is no explicitly configured max endpoint count.
     *
     * @see com.hazelcast.enterprise.wan.replication.WanReplicationProperties#MAX_ENDPOINTS
     */
    private static final int DEFAULT_MAX_ENDPOINTS = Integer.MAX_VALUE;
    /**
     * Number of times the connection manager will attempt to retrieve a connection which
     * is being initialised asynchronously
     */
    private static final int RETRY_CONNECTION_MAX = 10;
    /**
     * The time in milliseconds that the connection manager sleeps between
     * consecutive checks whether a connection to an endpoint has been established.
     */
    private static final int RETRY_CONNECTION_SLEEP_MILLIS = 1000;
    /**
     * The default group password if none is configured in the publisher properties.
     *
     * @see com.hazelcast.enterprise.wan.replication.WanReplicationProperties#GROUP_PASSWORD
     */
    private static final String DEFAULT_GROUP_PASS = "dev-pass";

    private final Node node;
    private final ILogger logger;
    private final ConcurrentMap<Address, WanConnectionWrapper> connectionPool =
            new ConcurrentHashMap<Address, WanConnectionWrapper>();
    private final List<Address> targetEndpoints = new CopyOnWriteArrayList<Address>();
    private final DiscoveryService discoveryService;
    private final ConstructorFunction<Address, WanConnectionWrapper> connectionConstructor =
            new ConstructorFunction<Address, WanConnectionWrapper>() {
                @Override
                public WanConnectionWrapper createNew(Address addr) {
                    final Connection conn = initConnection(addr);
                    if (conn == null) {
                        throw new RuntimeException("Connection was not established in expected time or was not authorized");
                    }
                    return new WanConnectionWrapper(addr, conn);
                }
            };
    private String groupName;
    private String password;
    private Integer maxEndpoints;
    private boolean useEndpointPrivateAddress;

    public WanConnectionManager(Node node, DiscoveryService discoveryService) {
        this.node = node;
        this.logger = node.getLogger(WanConnectionManager.class.getName());
        this.discoveryService = discoveryService;
    }

    /**
     * Initialise the connection manager. This will run discovery on the supplied {@link DiscoveryService} and
     * schedule a task to rerun discovery.
     *
     * @param groupName           the group name for the discovered endpoints
     * @param publisherProperties the configuration properties for the WAN publisher
     */
    public void init(String groupName, Map<String, Comparable> publisherProperties) {
        this.groupName = groupName;
        this.password = getProperty(GROUP_PASSWORD, publisherProperties, DEFAULT_GROUP_PASS);
        this.maxEndpoints = isNullOrEmpty(getProperty(ENDPOINTS, publisherProperties, ""))
                ? getProperty(MAX_ENDPOINTS, publisherProperties, WanConnectionManager.DEFAULT_MAX_ENDPOINTS)
                : WanConnectionManager.DEFAULT_MAX_ENDPOINTS;
        this.useEndpointPrivateAddress = getProperty(DISCOVERY_USE_ENDPOINT_PRIVATE_ADDRESS, publisherProperties, false);

        final Integer discoveryPeriodSeconds =
                getProperty(DISCOVERY_PERIOD, publisherProperties, WanConnectionManager.DEFAULT_DISCOVERY_TASK_PERIOD);

        try {
            addToTargetEndpoints(discoverEndpointAddresses());
        } catch (Exception e) {
            final String msg = "Failed to initialize WAN endpoint list";
            if (discoveryService instanceof PredefinedDiscoveryService) {
                throw new InvalidConfigurationException(msg, e);
            } else {
                logger.warning(msg, e);
            }
        }
        if (targetEndpoints.size() == 0) {
            final String msg = "There were no discovered nodes for WanPublisherConfig,"
                    + "please define endpoints statically or check the AWS and discovery config";
            if (discoveryService instanceof PredefinedDiscoveryService) {
                throw new InvalidConfigurationException(msg);
            } else {
                logger.warning(msg);
            }
        }

        node.nodeEngine.getExecutionService().scheduleWithRepetition(new TargetEndpointDiscoveryTask(),
                DISCOVERY_TASK_START_DELAY, discoveryPeriodSeconds, TimeUnit.SECONDS);
    }

    /** Add endpoints to the target endpoint list, respecting the {@link #maxEndpoints} property */
    private void addToTargetEndpoints(List<Address> addresses) {
        targetEndpoints.addAll(addresses.subList(0, min(maxEndpoints - targetEndpoints.size(), addresses.size())));
    }

    /**
     * Runs the discovery SPI implementation to return a list of addresses
     * to which the connection manager can connect to.
     *
     * @return the endpoint addresses
     * @see com.hazelcast.enterprise.wan.replication.WanReplicationProperties#DISCOVERY_USE_ENDPOINT_PRIVATE_ADDRESS
     */
    private List<Address> discoverEndpointAddresses() {
        final Iterable<DiscoveryNode> nodes = discoveryService.discoverNodes();
        final ArrayList<Address> addresses = new ArrayList<Address>();
        for (DiscoveryNode node : nodes) {
            final Address address = useEndpointPrivateAddress ? node.getPrivateAddress() : node.getPublicAddress();
            if (address != null) {
                addresses.add(address);
            } else {
                logger.finest("Discovery strategy returned a null address, ignoring...");
            }
        }
        return addresses;
    }

    /**
     * Returns either the connection wrapper for the requested {@code target} or for the first target in
     * the endpoint list if the provided {@code target} is not in the endpoint list. The method may return null
     * if this method fails to create a connection, either because it cannot connect in the expected time or it has
     * failed to authorize.
     * The method will check if the connection is alive before returning the wrapper.
     *
     * @param target the address for which a connection is requested
     * @return connection to the target address, the first endpoint if the target is not in the endpoint list or null
     */
    public WanConnectionWrapper getConnection(Address target) {
        final Address targetAddress = selectTarget(target);
        return getConnectionByTargetAddress(targetAddress);
    }

    public void removeTargetEndpoint(Address targetAddress, String reason, Throwable cause) {
        synchronized (targetEndpoints) {
            targetEndpoints.remove(targetAddress);
        }
        final WanConnectionWrapper wrapper = connectionPool.remove(targetAddress);
        if (wrapper != null) {
            try {
                wrapper.getConnection().close(reason, cause);
            } catch (Exception e) {
                logger.warning("Error closing connection", e);
            }
        }
    }

    /**
     * Return the first target endpoint address. If the known target endpoint list is empty,
     * wait for {@value RETRY_CONNECTION_SLEEP_MILLIS}. The method may return {@code null} if there are no known
     * target endpoints at this time (the target endpoint list is empty).
     *
     * @return the target endpoint address or null if there are no target endpoints at this time
     */
    private Address selectFirstTarget() {
        synchronized (targetEndpoints) {
            if (!targetEndpoints.isEmpty()) {
                return targetEndpoints.get(0);
            }
        }
        try {
            targetEndpoints.wait(RETRY_CONNECTION_SLEEP_MILLIS);
        } catch (InterruptedException e) {
            logger.finest("WanConnectionManager wait interrupted.");
        }
        return null;
    }

    /**
     * Return the given {@code target} if it is contained in the target endpoint list for this manager, otherwise return
     * the first target.
     *
     * @param target the target which is checked against this managers endpoint list
     * @return the provided target or the first target
     */
    private Address selectTarget(Address target) {
        synchronized (targetEndpoints) {
            return targetEndpoints.contains(target) ? target : selectFirstTarget();
        }
    }

    /**
     * Return an existing connection or create a new one. The method will return null if this method fails to create a
     * connection, either because it cannot connect in the expected time or it has failed to authorize.
     * The method will check if the connection is alive before returning the wrapper.
     *
     * @param targetAddress the address to connect to
     * @return the connection or null if it the connection wasn't established
     */
    private WanConnectionWrapper getConnectionByTargetAddress(Address targetAddress) {
        if (targetAddress == null) {
            return null;
        }
        try {
            final WanConnectionWrapper wrapper =
                    getOrPutSynchronized(connectionPool, targetAddress, connectionPool, connectionConstructor);
            if (wrapper.getConnection().isAlive()) {
                return wrapper;
            } else {
                removeTargetEndpoint(targetAddress, "Connection to WAN endpoint " + targetAddress + " is dead", null);
            }
        } catch (Throwable e) {
            final String msg = "Failed to connect to WAN endpoint : " + targetAddress;
            logger.warning(msg, e);
            removeTargetEndpoint(targetAddress, msg, e);
        }
        return null;
    }


    /**
     * Attempt to create the connection to the {@code targetAddress} and authenticate.
     * Since the connection creation is asynchronous, it will try waiting for
     * {@value RETRY_CONNECTION_SLEEP_MILLIS} millis up to {@value RETRY_CONNECTION_MAX} times before giving up.
     * It may return null if the connection was not established in time or if the authorization failed.
     *
     * @param targetAddress the address to connect to
     * @return the established connection or null if the connection was not established
     */
    private Connection initConnection(Address targetAddress) {
        try {
            final ConnectionManager connectionManager = node.getConnectionManager();
            Connection conn = connectionManager.getOrConnect(targetAddress);
            for (int i = 0; i < RETRY_CONNECTION_MAX; i++) {
                if (conn == null) {
                    MILLISECONDS.sleep(RETRY_CONNECTION_SLEEP_MILLIS);
                }
                conn = connectionManager.getConnection(targetAddress);
            }
            if (conn != null) {
                return authorizeConnection(conn);
            }
        } catch (InterruptedException ie) {
            logger.finest("Sleep interrupted", ie);
        }
        return null;
    }

    private boolean checkAuthorization(String groupName, String groupPassword, Address target) {
        Operation authorizationCall = new AuthorizationOp(groupName, groupPassword);
        OperationService operationService = node.nodeEngine.getOperationService();
        String serviceName = EnterpriseWanReplicationService.SERVICE_NAME;
        InvocationBuilder invocationBuilder = operationService.createInvocationBuilder(serviceName, authorizationCall, target);
        Future<Boolean> future = invocationBuilder.setTryCount(1).invoke();
        try {
            return future.get();
        } catch (Exception ignored) {
            logger.finest(ignored);
        }
        return false;
    }

    /**
     * Perform authorization on the connection. If the authorization failed, close the connection and log the authorization
     * failure.
     *
     * @param conn the connection to authorize
     * @return the authorized connection or null if the authorization failed
     */
    private Connection authorizeConnection(Connection conn) {
        boolean authorized = checkAuthorization(groupName, password, conn.getEndPoint());
        if (!authorized) {
            String msg = "Invalid groupName or groupPassword!";
            conn.close(msg, null);
            if (logger != null) {
                logger.severe(msg);
            }
            return null;
        }
        return conn;
    }

    /**
     * Return a snapshot of the list of currently known target endpoints to which replication is made. Some of them can
     * currently have dead connections and are about to be removed.
     *
     * @return the list of currently known target endpoints
     * @see #removeTargetEndpoint(Address, String, Throwable)
     */
    public List<Address> getTargetEndpoints() {
        return new ArrayList<Address>(targetEndpoints);
    }

    /**
     * Performs runtime discovery of new WAN target endpoints. New discovered nodes are added to the target endpoint list.
     * The connections will be established during normal WAN operations.
     */
    private class TargetEndpointDiscoveryTask implements Runnable {
        @Override
        public void run() {
            try {
                final List<Address> discoveredNodes = discoverEndpointAddresses();
                synchronized (targetEndpoints) {
                    // the following steps could be simplified to
                    // targetEndpoints.clear(); targetEndpoints.addAll(discoveredNodes);
                    // but we additionally try to satisfy two properties :
                    // - avoid clearing the endpoint list and leaving it empty (even temporarily)
                    // - respect the maxEndpoints property

                    // retain all discovered, removing others
                    targetEndpoints.retainAll(discoveredNodes);
                    // remove known live and discovered endpoints
                    discoveredNodes.removeAll(targetEndpoints);

                    // add any newly discovered nodes
                    if (discoveredNodes.size() > 0) {
                        addToTargetEndpoints(discoveredNodes);
                        targetEndpoints.notify();
                    }
                }
            } catch (Exception e) {
                logger.fine("Failed to discover new nodes for WAN replication", e);
            }
        }
    }
}
