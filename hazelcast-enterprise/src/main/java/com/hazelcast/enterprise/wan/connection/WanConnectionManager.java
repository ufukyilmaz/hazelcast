package com.hazelcast.enterprise.wan.connection;

import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.enterprise.wan.replication.WanConfigurationContext;
import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.instance.Node;
import com.hazelcast.internal.cluster.impl.operations.AuthorizationOp;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.ConnectionListener;
import com.hazelcast.nio.EndpointManager;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.discovery.DiscoveryNode;
import com.hazelcast.spi.discovery.impl.PredefinedDiscoveryService;
import com.hazelcast.spi.discovery.integration.DiscoveryService;
import com.hazelcast.spi.impl.operationservice.OperationService;
import com.hazelcast.util.ConstructorFunction;
import com.hazelcast.util.EmptyStatement;
import com.hazelcast.wan.WanReplicationPublisher;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.instance.ProtocolType.WAN;
import static com.hazelcast.util.ConcurrencyUtil.getOrPutSynchronized;
import static com.hazelcast.wan.WanReplicationService.SERVICE_NAME;
import static java.lang.Math.min;
import static java.lang.Thread.currentThread;
import static java.util.Collections.newSetFromMap;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Maintains connections for WAN replication for a single
 * {@link WanReplicationPublisher}.
 */
public class WanConnectionManager implements ConnectionListener {
    /**
     * Delay in seconds between the initialisation of the connection manager and
     * the time the discovery task is first run.
     *
     * @see TargetEndpointDiscoveryTask
     */
    private static final int DISCOVERY_TASK_START_DELAY = 10;

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

    private final Node node;
    private final ILogger logger;
    private final ConcurrentMap<Address, WanConnectionWrapper> connectionPool =
            new ConcurrentHashMap<Address, WanConnectionWrapper>();
    private final List<Address> targetEndpoints = new CopyOnWriteArrayList<Address>();
    private final DiscoveryService discoveryService;
    private final Set<Address> connectionsInProgress = newSetFromMap(new ConcurrentHashMap<Address, Boolean>());
    private final ConstructorFunction<Address, WanConnectionWrapper> connectionConstructor =
            new ConstructorFunction<Address, WanConnectionWrapper>() {
                @Override
                public WanConnectionWrapper createNew(Address addr) {
                    final Connection conn = initConnection(addr);
                    if (conn == null) {
                        throw new RuntimeException(
                                "Connection was not established in expected time or was not authorized");
                    }
                    return new WanConnectionWrapper(addr, conn);
                }
            };
    private WanConfigurationContext configurationContext;
    private EndpointQualifier endpointQualifier;
    private volatile boolean running = true;

    public WanConnectionManager(Node node, DiscoveryService discoveryService) {
        this.node = node;
        this.logger = node.getLogger(WanConnectionManager.class.getName());
        this.discoveryService = discoveryService;
    }

    /**
     * Initialise the connection manager. This will run discovery on the supplied
     * {@link DiscoveryService} and schedule a task to rerun discovery.
     *
     * @param configurationContext the configuration context for the WAN publisher
     */
    public void init(WanConfigurationContext configurationContext) {
        this.configurationContext = configurationContext;
        String endpointIdentifier = configurationContext.getPublisherConfig().getEndpoint();
        this.endpointQualifier = endpointIdentifier == null ? EndpointQualifier.MEMBER
                : EndpointQualifier.resolve(WAN, endpointIdentifier);
        node.networkingService.getEndpointManager(endpointQualifier).addConnectionListener(this);

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
                    + "please define endpoints statically or check the discovery config";
            if (discoveryService instanceof PredefinedDiscoveryService) {
                throw new InvalidConfigurationException(msg);
            } else {
                logger.warning(msg);
            }
        }

        node.getNodeEngine().getExecutionService().scheduleWithRepetition(new TargetEndpointDiscoveryTask(),
                DISCOVERY_TASK_START_DELAY, configurationContext.getDiscoveryPeriodSeconds(), TimeUnit.SECONDS);
    }

    /**
     * Returns a list of currently live endpoints. It will sleep until the list
     * contains at least one endpoint or {@link #running} is {@code false} (this
     * connection manager is shutting down) at which point it can return an empty
     * list.
     */
    public List<Address> awaitAndGetTargetEndpoints() {
        while (running) {
            final List<Address> endpoints = getTargetEndpoints();
            if (!endpoints.isEmpty()) {
                return endpoints;
            }
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                // the lifecycle of this class is managed by the AbstractWanReplication class from which the shutdown request
                // is passed down via the running flag by calling the shutdown method
                // this class is expected to be operational and this method to continue until shutdown is explicitly called
                EmptyStatement.ignore(e);
            }
        }
        return Collections.emptyList();
    }

    /**
     * Shuts down this connection manager.
     */
    public void shutdown() {
        running = false;
    }

    /**
     * Adds endpoints to the target endpoint list, respecting the
     * {@link WanConfigurationContext#getMaxEndpoints()} property.
     */
    private void addToTargetEndpoints(List<Address> addresses) {
        final int endpointCount = min(configurationContext.getMaxEndpoints() - targetEndpoints.size(), addresses.size());
        targetEndpoints.addAll(addresses.subList(0, endpointCount));
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
            final Address address = configurationContext.isUseEndpointPrivateAddress()
                    ? node.getPrivateAddress()
                    : node.getPublicAddress();
            if (address != null) {
                addresses.add(address);
            } else {
                logger.finest("Discovery strategy returned a null address, ignoring...");
            }
        }
        return addresses;
    }

    /**
     * Returns either the connection wrapper for the requested {@code target}
     * or for the first target in the endpoint list if the provided
     * {@code target} is not in the endpoint list. The method may return
     * {@code null} if this method fails to create a connection, either because
     * it cannot connect in the expected time or it has failed to authorize.
     * The method will check if the connection is alive before returning the
     * wrapper.
     *
     * @param target the address for which a connection is requested
     * @return connection to the target address, the first endpoint if the
     * target is not in the endpoint list or {@code null}
     */
    public WanConnectionWrapper getConnection(Address target) {
        final Address targetAddress = selectTarget(target);
        return getConnectionByTargetAddress(targetAddress);
    }

    /**
     * Tests if connected to a configured target cluster endpoint
     * over WAN. This method iterates over the target endpoints until an
     * alive connection is found or no endpoint is left to test.
     *
     * @return {@code true} if there is at least one alive connection, {@code false} otherwise
     */
    public boolean isConnected() {
        for (Address target : targetEndpoints) {
            final WanConnectionWrapper wrapper = connectionPool.get(target);
            if (wrapper != null && wrapper.getConnection().isAlive()) {
                return true;
            }
        }
        return false;
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
     * Return the first target endpoint address. If the known target endpoint
     * list is empty, wait for {@value RETRY_CONNECTION_SLEEP_MILLIS}. The method
     * may return {@code null} if there are no known target endpoints at this
     * time (the target endpoint list is empty).
     *
     * @return the target endpoint address or null if there are no target
     * endpoints at this time
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
            currentThread().interrupt();
            logger.finest("WanConnectionManager wait interrupted.");
        }
        return null;
    }

    /**
     * Return the given {@code target} if it is contained in the target
     * endpoint list for this manager, otherwise return the first target.
     *
     * @param target the target which is checked against this managers endpoint
     *               list
     * @return the provided target or the first target
     */
    private Address selectTarget(Address target) {
        synchronized (targetEndpoints) {
            return targetEndpoints.contains(target) ? target : selectFirstTarget();
        }
    }

    /**
     * Return an existing connection or create a new one. The method will
     * return {@code null} if this method fails to create a connection, either
     * because it cannot connect in the expected time or it has failed to
     * authorize.
     * The method will check if the connection is alive before returning the
     * wrapper.
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
     * Attempt to create the connection to the {@code targetAddress} and
     * authenticate.
     * Since the connection creation is asynchronous, it will try waiting for
     * {@value RETRY_CONNECTION_SLEEP_MILLIS} millis up to
     * {@value RETRY_CONNECTION_MAX} times before giving up.
     * It may return null if the connection was not established in time or if
     * the authorization failed.
     *
     * @param targetAddress the address to connect to
     * @return the established connection or null if the connection was not established
     */
    private Connection initConnection(Address targetAddress) {
        try {
            connectionsInProgress.add(targetAddress);
            EndpointManager endpointManager = node.getEndpointManager(endpointQualifier);
            if (endpointManager == null) {
                endpointManager = node.getEndpointManager();
            }
            Connection conn = endpointManager.getOrConnect(targetAddress);
            for (int i = 0; i < RETRY_CONNECTION_MAX; i++) {
                if (conn == null) {
                    MILLISECONDS.sleep(RETRY_CONNECTION_SLEEP_MILLIS);
                }
                conn = endpointManager.getOrConnect(targetAddress);
            }
            if (conn != null) {
                return authorizeConnection(conn);
            }
        } catch (InterruptedException ie) {
            currentThread().interrupt();
            logger.finest("Sleep interrupted", ie);
        } finally {
            connectionsInProgress.remove(targetAddress);
        }
        return null;
    }

    /**
     * Runs an authorization operation against the given {@code target} address
     * with the given {@code groupName} and {@code groupPassword}.
     *
     * @param groupName     expected group name
     * @param groupPassword expected password
     * @param target        the target to validate group name and password against
     * @return {@code true} if authorization passed, {@code false} otherwise
     */
    private boolean checkAuthorization(String groupName, String groupPassword, Address target) {
        Operation authorizationCall = new AuthorizationOp(groupName, groupPassword);
        Future<Boolean> future = node.getNodeEngine().getOperationService()
                                                .createInvocationBuilder(SERVICE_NAME, authorizationCall, target)
                                                .setTryCount(1)
                                                .setEndpointManager(node.getEndpointManager(endpointQualifier))
                                                .invoke();
        try {
            return future.get();
        } catch (Exception ignored) {
            logger.finest(ignored);
        }
        return false;
    }

    /**
     * Perform authorization on the connection. If the authorization failed,
     * close the connection and log the authorization failure.
     *
     * @param conn the connection to authorize
     * @return the authorized connection or null if the authorization failed
     */
    private Connection authorizeConnection(Connection conn) {
        String groupName = configurationContext.getGroupName();
        boolean authorized = checkAuthorization(
                groupName,
                configurationContext.getPassword(),
                conn.getEndPoint());
        if (!authorized) {
            final String msg = "WAN authorization failed for groupName " + groupName
                    + " and target " + conn.getEndPoint();
            conn.close(msg, null);
            if (logger != null) {
                logger.severe(msg);
            }
            return null;
        }
        return conn;
    }

    /**
     * Return a snapshot of the list of currently known target endpoints to
     * which replication is made. Some of them can currently have dead
     * connections and are about to be removed.
     *
     * @return the list of currently known target endpoints
     * @see #removeTargetEndpoint(Address, String, Throwable)
     */
    public List<Address> getTargetEndpoints() {
        return new ArrayList<Address>(targetEndpoints);
    }

    @Override
    public void connectionAdded(Connection connection) {
        // NOOP
    }

    @Override
    public void connectionRemoved(Connection connection) {
        Address endpoint = connection.getEndPoint();
        WanConnectionWrapper wrapper = connectionPool.remove(endpoint);
        OperationService operationService = node.nodeEngine.getOperationService();
        if (wrapper != null || connectionsInProgress.contains(endpoint)) {
            operationService.onEndpointLeft(endpoint);
        }
    }

    /**
     * Performs runtime discovery of new WAN target endpoints. New discovered
     * nodes are added to the target endpoint list.
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
