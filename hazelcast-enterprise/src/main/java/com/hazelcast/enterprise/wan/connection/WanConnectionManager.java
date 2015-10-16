package com.hazelcast.enterprise.wan.connection;

import com.hazelcast.cluster.impl.operations.AuthorizationOperation;
import com.hazelcast.enterprise.wan.EnterpriseWanReplicationService;
import com.hazelcast.instance.Node;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.ConnectionManager;
import com.hazelcast.spi.InvocationBuilder;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.util.AddressUtil;

import java.net.UnknownHostException;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Maintains connections for WAN replication
 */
public class WanConnectionManager {

    private static final int RETRY_CONNECTION_MAX = 10;
    private static final int RETRY_CONNECTION_SLEEP_MILLIS = 1000;
    private static final int FAILURE_MONITOR_START_DELAY = 10;
    private static final int FAILURE_MONITOR_PERIOD = 10;

    private Node node;
    private ILogger logger;
    private String groupName;
    private String password;
    private final ConcurrentMap<String, Connection> connectionPool = new ConcurrentHashMap<String, Connection>();
    private final List<String> targetAddressList = new CopyOnWriteArrayList<String>();

    private final Set<String> failedAddressSet = new ConcurrentSkipListSet<String>();
    private final int defaultPort;

    public WanConnectionManager(Node node) {
        this.node = node;
        this.defaultPort = node.getConfig().getNetworkConfig().getPort();
        this.logger = node.getLogger(WanConnectionManager.class.getName());
    }

    public void init(String groupName, String password, List<String> targets) {
        this.groupName = groupName;
        this.password = password;
        targetAddressList.addAll(targets);
        node.nodeEngine.getExecutionService().scheduleAtFixedRate(new FailureMonitor(), FAILURE_MONITOR_START_DELAY,
                FAILURE_MONITOR_PERIOD, TimeUnit.SECONDS);
    }

    public WanConnectionWrapper getConnection(int partitionId) throws InterruptedException {
        String targetAddress = selectTarget(partitionId);
        Connection conn = getConnectionByTargetAddress(targetAddress);
        return new WanConnectionWrapper(targetAddress, groupName, conn);
    }

    public WanConnectionWrapper getConnection(String target) throws InterruptedException {
        String targetAddress = selectTarget(target);
        Connection conn = getConnectionByTargetAddress(targetAddress);
        return new WanConnectionWrapper(targetAddress, groupName, conn);
    }

    public void reportFailedConnection(String targetAddress) {
        synchronized (targetAddressList) {
            targetAddressList.remove(targetAddress);
        }
        failedAddressSet.add(targetAddress);
    }

    private String selectTarget(int partitionId) {
        String targetAddress = null;
        synchronized (targetAddressList) {
            if (!targetAddressList.isEmpty()) {
                targetAddress = targetAddressList.get(partitionId % targetAddressList.size());
            } else {
                try {
                    targetAddressList.wait(RETRY_CONNECTION_SLEEP_MILLIS);
                } catch (InterruptedException e) {
                    logger.finest("WanConnectionManager wait interrupted.");
                }
            }
        }
        return targetAddress;
    }

    private String selectTarget(String target) {
        String targetAddress;
        synchronized (targetAddressList) {
            if (targetAddressList.contains(target)) {
                targetAddress = target;
            } else {
                targetAddress = selectTarget(0);
            }
        }
        return targetAddress;
    }

    private Connection getConnectionByTargetAddress(String targetAddress) {
        Connection conn = null;
        if (targetAddress != null) {
            conn = connectionPool.get(targetAddress);
            if (conn == null) {
                try {
                    final AddressUtil.AddressHolder addressHolder = AddressUtil.getAddressHolder(targetAddress, defaultPort);
                    final Address target = new Address(addressHolder.getAddress(), addressHolder.getPort());
                    final ConnectionManager connectionManager = node.getConnectionManager();
                    conn = connectionManager.getOrConnect(target);
                    for (int i = 0; i < RETRY_CONNECTION_MAX; i++) {
                        if (conn == null) {
                            Thread.sleep(RETRY_CONNECTION_SLEEP_MILLIS);
                        }
                        conn = connectionManager.getConnection(target);
                    }
                    if (conn != null) {
                        conn = authorizeConnection(conn);
                    }
                    if (conn != null) {
                        connectionPool.put(targetAddress, conn);
                    }
                } catch (InterruptedException ie) {
                    logger.finest("Sleep interrupted", ie);
                } catch (Throwable t) {
                    logger.warning("Failed to connect wan replication endpoint : " + targetAddress, t);
                }
            }
            if (conn == null || !conn.isAlive()) {
                reportFailedConnection(targetAddress);
                connectionPool.remove(targetAddress);
            }
        }
        return conn;
    }

    private boolean checkAuthorization(String groupName, String groupPassword, Address target) {
        Operation authorizationCall = new AuthorizationOperation(groupName, groupPassword);
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

    private Connection authorizeConnection(Connection conn) {
        boolean authorized = checkAuthorization(groupName, password, conn.getEndPoint());
        if (!authorized) {
            conn.close();
            if (logger != null) {
                logger.severe("Invalid groupName or groupPassword! ");
            }
            return null;
        }
        return conn;
    }

    private Connection getOrConnect(String targetAddress) throws UnknownHostException {
        final AddressUtil.AddressHolder addressHolder = AddressUtil.getAddressHolder(targetAddress, defaultPort);
        final Address target = new Address(addressHolder.getAddress(), addressHolder.getPort());
        final ConnectionManager connectionManager = node.getConnectionManager();
        return connectionManager.getOrConnect(target);
    }

    public Set<String> getFailedAddressSet() {
        return failedAddressSet;
    }
    /**
     * Maintains WAN target address set
     */
    private class FailureMonitor implements Runnable {
        @Override
        public void run() {
            Iterator<String> iter = failedAddressSet.iterator();
            while (iter.hasNext()) {
                String targetAddress = iter.next();
                try {
                    Connection conn = getOrConnect(targetAddress);
                    if (conn != null) {
                        synchronized (targetAddressList) {
                            targetAddressList.add(targetAddress);
                            targetAddressList.notify();
                        }
                        failedAddressSet.remove(targetAddress);
                    }
                } catch (Throwable t) {
                    logger.info("Retry to connect WAN replication end point failed: " + targetAddress);
                }
            }
        }
    }
}
