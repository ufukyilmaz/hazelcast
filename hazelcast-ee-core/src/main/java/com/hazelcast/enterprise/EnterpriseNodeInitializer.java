package com.hazelcast.enterprise;

import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.SocketInterceptorConfig;
import com.hazelcast.elasticmemory.InstanceStorageFactory;
import com.hazelcast.elasticmemory.SingletonStorageFactory;
import com.hazelcast.elasticmemory.StorageFactory;
import com.hazelcast.enterprise.wan.EnterpriseWanReplicationService;
import com.hazelcast.instance.DefaultNodeInitializer;
import com.hazelcast.instance.Node;
import com.hazelcast.instance.NodeInitializer;
import com.hazelcast.nio.MemberSocketInterceptor;
import com.hazelcast.nio.SocketInterceptor;
import com.hazelcast.security.SecurityContext;
import com.hazelcast.security.SecurityContextImpl;
import com.hazelcast.storage.Storage;
import com.hazelcast.wan.WanReplicationService;

import java.util.Calendar;
import java.util.Date;
import java.util.logging.Level;

/**
 * This class is the enterprise system hook to allow injection of enterprise services into Hazelcast subsystems
 */
public class EnterpriseNodeInitializer
        extends DefaultNodeInitializer
        implements NodeInitializer {

    private static final int HOUR_OF_DAY = 23;
    private static final int MINUTE = 59;
    private static final int SECOND = 59;

    private Storage storage;
    private volatile License license;
    private SecurityContext securityContext;
    private boolean securityEnabled;
    private MemberSocketInterceptor memberSocketInterceptor;

    public EnterpriseNodeInitializer() {
        super();
    }

    public void beforeInitialize(Node node) {
        this.node = node;
        logger = node.getLogger("com.hazelcast.enterprise.initializer");
        Date validUntil;
        try {
            logger.log(Level.INFO, "Checking Hazelcast Enterprise license...");
            validUntil = validateLicense(node);
        } catch (Exception e) {
            throw new InvalidLicenseError();
        }

        if (license == null || validUntil == null || System.currentTimeMillis() > validUntil.getTime()) {
            throw new TrialLicenseExpiredError();
        }

        systemLogger = node.getLogger("com.hazelcast.system");
        parseSystemProps();
        securityEnabled = node.getConfig().getSecurityConfig().isEnabled();

        if (node.groupProperties.ELASTIC_MEMORY_ENABLED.getBoolean()) {
            StorageFactory storageFactory;
            if (node.groupProperties.ELASTIC_MEMORY_SHARED_STORAGE.getBoolean()) {
                logger.log(Level.WARNING, "Using SingletonStorageFactory for Hazelcast Elastic Memory...");
                storageFactory = new SingletonStorageFactory();
            } else {
                storageFactory = new InstanceStorageFactory(node);
            }

            logger.log(Level.INFO, "Initializing node off-heap storage.");
            storage = storageFactory.createStorage();
        }
        getSocketInterceptor(node.config.getNetworkConfig());
    }

    private void getSocketInterceptor(NetworkConfig networkConfig) {
        SocketInterceptorConfig sic = networkConfig.getSocketInterceptorConfig();
        SocketInterceptor implementation = null;
        if (sic != null && sic.isEnabled()) {
            implementation = (SocketInterceptor) sic.getImplementation();
            if (implementation == null && sic.getClassName() != null) {
                try {
                    implementation = (SocketInterceptor) Class.forName(sic.getClassName()).newInstance();
                } catch (Throwable e) {
                    logger.severe("SocketInterceptor class cannot be instantiated!" + sic.getClassName(), e);
                }
            }
            if (implementation != null) {
                if (!(implementation instanceof MemberSocketInterceptor)) {
                    logger.severe("SocketInterceptor must be instance of " + MemberSocketInterceptor.class.getName());
                    implementation = null;
                }
            }
        }

        memberSocketInterceptor = (MemberSocketInterceptor) implementation;
        if (memberSocketInterceptor != null) {
            logger.info("SocketInterceptor is enabled");
            memberSocketInterceptor.init(sic.getProperties());
        }
    }

    public void printNodeInfo(Node node) {
        systemLogger.log(Level.INFO,
                "Hazelcast Enterprise " + version + " (" + build + ") starting at " + node.getThisAddress());
        systemLogger.log(Level.INFO, "Copyright (C) 2008-2014 Hazelcast.com");
    }

    public void afterInitialize(Node node) {
        if (license == null) {
            logger.log(Level.SEVERE, "Hazelcast Enterprise license could not be found!");
            node.shutdown(true);
            return;
        }
        final int count = node.getClusterService().getSize();
        if (count > license.nodes) {
            logger.log(Level.SEVERE,
                    "Exceeded maximum number of nodes allowed in Hazelcast Enterprise license! Max: " + license.nodes
                            + ", Current: " + count
            );
            node.shutdown(true);
        }
    }

    public License getLicense() {
        return license;
    }

    public SecurityContext getSecurityContext() {
        if (securityEnabled && securityContext == null) {
            securityContext = new SecurityContextImpl(node);
        }
        return securityContext;
    }

    @Override
    public MemberSocketInterceptor getMemberSocketInterceptor() {
        return memberSocketInterceptor;
    }

    public Storage getOffHeapStorage() {
        if (storage == null) {
            throw new IllegalStateException(
                    "Offheap storage is not enabled! " + "Please set 'hazelcast.elastic.memory.enabled' to true");
        }
        return storage;
    }

    @Override
    public WanReplicationService geWanReplicationService() {
        return new EnterpriseWanReplicationService(node);
    }

    public void destroy() {
        super.destroy();
        license = null;
        if (storage != null) {
            logger.log(Level.FINEST, "Destroying node off-heap storage.");
            storage.destroy();
            storage = null;
        }
    }

    private Date validateLicense(Node node) {
        Date validUntil;
        String licenseKey = node.groupProperties.ENTERPRISE_LICENSE_KEY.getString();
        if (licenseKey == null || "".equals(licenseKey)) {
            licenseKey = node.getConfig().getLicenseKey();
        }
        license = KG.ex(licenseKey != null ? licenseKey.toCharArray() : null);
        Calendar cal = Calendar.getInstance();
        cal.set(license.year, license.month, license.day, HOUR_OF_DAY, MINUTE, SECOND);
        validUntil = cal.getTime();
        logger.log(Level.INFO,
                "Licensed type: " + (license.full ? "Full" : "Trial") + ", Valid until: " + validUntil + ", Max nodes: "
                        + license.nodes
        );
        return validUntil;
    }
}
