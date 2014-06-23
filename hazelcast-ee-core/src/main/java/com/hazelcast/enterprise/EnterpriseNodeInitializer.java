package com.hazelcast.enterprise;

import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.SSLConfig;
import com.hazelcast.config.SocketInterceptorConfig;
import com.hazelcast.config.SymmetricEncryptionConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.elasticmemory.InstanceStorageFactory;
import com.hazelcast.elasticmemory.SingletonStorageFactory;
import com.hazelcast.elasticmemory.StorageFactory;
import com.hazelcast.enterprise.wan.EnterpriseWanReplicationService;
import com.hazelcast.instance.DefaultNodeInitializer;
import com.hazelcast.instance.Node;
import com.hazelcast.instance.NodeInitializer;
import com.hazelcast.nio.CipherHelper;
import com.hazelcast.nio.IOService;
import com.hazelcast.nio.MemberSocketInterceptor;
import com.hazelcast.nio.Packet;
import com.hazelcast.nio.SocketInterceptor;
import com.hazelcast.nio.ssl.BasicSSLContextFactory;
import com.hazelcast.nio.ssl.SSLContextFactory;
import com.hazelcast.nio.ssl.SSLSocketChannelWrapper;
import com.hazelcast.nio.tcp.PacketReader;
import com.hazelcast.nio.tcp.PacketWriter;
import com.hazelcast.nio.tcp.SocketChannelWrapper;
import com.hazelcast.nio.tcp.SocketChannelWrapperFactory;
import com.hazelcast.nio.tcp.TcpIpConnection;
import com.hazelcast.security.SecurityContext;
import com.hazelcast.security.SecurityContextImpl;
import com.hazelcast.storage.Storage;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.wan.WanReplicationService;

import javax.crypto.Cipher;
import javax.crypto.ShortBufferException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
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

    @Override
    public SocketChannelWrapperFactory getSocketChannelWrapperFactory() {
        final NetworkConfig networkConfig = node.config.getNetworkConfig();
        SSLConfig sslConfig = networkConfig.getSSLConfig();
        if (sslConfig != null && sslConfig.isEnabled()) {
            logger.info("SSL is enabled");
            return new SSLSocketChannelWrapperFactory(networkConfig);
        }
        return super.getSocketChannelWrapperFactory();
    }

    @Override
    public PacketReader createPacketReader(final TcpIpConnection connection, final IOService ioService) {
        final NetworkConfig networkConfig = node.config.getNetworkConfig();
        final SymmetricEncryptionConfig symmetricEncryptionConfig = networkConfig.getSymmetricEncryptionConfig();

        if (symmetricEncryptionConfig != null && symmetricEncryptionConfig.isEnabled()) {
            logger.info("Reader started with SymmetricEncryption");
            return new SymmetricCipherPacketReader(connection, ioService);
        }
        return super.createPacketReader(connection, ioService);
    }

    @Override
    public PacketWriter createPacketWriter(final TcpIpConnection connection, final IOService ioService) {
        final NetworkConfig networkConfig = node.config.getNetworkConfig();
        final SymmetricEncryptionConfig symmetricEncryptionConfig = networkConfig.getSymmetricEncryptionConfig();

        if (symmetricEncryptionConfig != null && symmetricEncryptionConfig.isEnabled()) {
            logger.info("Writer started with SymmetricEncryption");
            return new SymmetricCipherPacketWriter(connection, ioService);
        }
        return super.createPacketWriter(connection, ioService);
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

    static class SSLSocketChannelWrapperFactory implements SocketChannelWrapperFactory {
        final SSLContextFactory sslContextFactory;

        SSLSocketChannelWrapperFactory(NetworkConfig networkConfig) {
            final SSLConfig sslConfig = networkConfig.getSSLConfig();
            final SymmetricEncryptionConfig symmetricEncryptionConfig = networkConfig.getSymmetricEncryptionConfig();
            if (symmetricEncryptionConfig != null && symmetricEncryptionConfig.isEnabled()) {
                throw new RuntimeException("SSL and SymmetricEncryption cannot be both enabled!");
            }
            SSLContextFactory sslContextFactoryObject = (SSLContextFactory) sslConfig.getFactoryImplementation();
            try {
                String factoryClassName = sslConfig.getFactoryClassName();
                if (sslContextFactoryObject == null && factoryClassName != null) {
                    sslContextFactoryObject = (SSLContextFactory) Class.forName(factoryClassName).newInstance();
                }
                if (sslContextFactoryObject == null) {
                    sslContextFactoryObject = new BasicSSLContextFactory();
                }
                sslContextFactoryObject.init(sslConfig.getProperties());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            sslContextFactory = sslContextFactoryObject;
        }

        @Override
        public SocketChannelWrapper wrapSocketChannel(SocketChannel socketChannel, boolean client) throws Exception {
            return new SSLSocketChannelWrapper(sslContextFactory.getSSLContext(), socketChannel, client);
        }

        @Override
        public boolean isSSlEnabled() {
            return true;
        }
    }

    class SymmetricCipherPacketReader extends DefaultPacketReader {

        private static final int CONST_BUFFER_NO = 4;

        int size = -1;
        final Cipher cipher;
        ByteBuffer cipherBuffer = ByteBuffer.allocate(ioService.getSocketReceiveBufferSize() * IOService.KILO_BYTE);

        SymmetricCipherPacketReader(TcpIpConnection connection, IOService ioService) {
            super(connection, ioService);
            cipher = init();
        }

        Cipher init() {
            Cipher c;
            try {
                c = CipherHelper.createSymmetricReaderCipher(ioService.getSymmetricEncryptionConfig());
            } catch (Exception e) {
                logger.severe("Symmetric Cipher for ReadHandler cannot be initialized.", e);
                CipherHelper.handleCipherException(e, connection);
                throw ExceptionUtil.rethrow(e);
            }
            return c;
        }

        @Override
        public void readPacket(ByteBuffer inBuffer) throws Exception {
            while (inBuffer.hasRemaining()) {
                try {
                    if (size == -1) {
                        if (inBuffer.remaining() < CONST_BUFFER_NO) {
                            return;
                        }
                        size = inBuffer.getInt();
                        if (cipherBuffer.capacity() < size) {
                            cipherBuffer = ByteBuffer.allocate(size);
                        }
                    }
                    int remaining = inBuffer.remaining();
                    if (remaining < size) {
                        cipher.update(inBuffer, cipherBuffer);
                        size -= remaining;
                    } else if (remaining == size) {
                        cipher.doFinal(inBuffer, cipherBuffer);
                        size = -1;
                    } else {
                        int oldLimit = inBuffer.limit();
                        int newLimit = inBuffer.position() + size;
                        inBuffer.limit(newLimit);
                        cipher.doFinal(inBuffer, cipherBuffer);
                        inBuffer.limit(oldLimit);
                        size = -1;
                    }
                } catch (ShortBufferException e) {
                    logger.warning(e);
                }
                cipherBuffer.flip();
                while (cipherBuffer.hasRemaining()) {
                    if (packet == null) {
                        packet = obtainPacket();
                    }
                    boolean complete = packet.readFrom(cipherBuffer);
                    if (complete) {
                        packet.setConn(connection);
                        ioService.handleMemberPacket(packet);
                        packet = null;
                    }
                }
                cipherBuffer.clear();
            }
        }
    }

    private class SymmetricCipherPacketWriter implements PacketWriter {

        private static final int CONST_BUFFER_NO = 4;

        final IOService ioService;
        final TcpIpConnection connection;
        final Cipher cipher;
        ByteBuffer packetBuffer;
        boolean packetWritten;

        SymmetricCipherPacketWriter(TcpIpConnection connection, IOService ioService) {
            this.connection = connection;
            this.ioService = ioService;
            packetBuffer = ByteBuffer.allocate(ioService.getSocketSendBufferSize() * IOService.KILO_BYTE);
            cipher = init();
        }

        private Cipher init() {
            Cipher c;
            try {
                c = CipherHelper.createSymmetricWriterCipher(ioService.getSymmetricEncryptionConfig());
            } catch (Exception e) {
                logger.severe("Symmetric Cipher for WriteHandler cannot be initialized.", e);
                CipherHelper.handleCipherException(e, connection);
                throw ExceptionUtil.rethrow(e);
            }
            return c;
        }

        @Override
        public boolean writePacket(Packet packet, ByteBuffer socketBuffer) throws Exception {
            if (!packetWritten) {
                if (socketBuffer.remaining() < CONST_BUFFER_NO) {
                    return false;
                }
                int size = cipher.getOutputSize(packet.size());
                socketBuffer.putInt(size);

                if (packetBuffer.capacity() < packet.size()) {
                    packetBuffer = ByteBuffer.allocate(packet.size());
                }
                if (!packet.writeTo(packetBuffer)) {
                    throw new HazelcastException("Packet didn't fit into the buffer!");
                }
                packetBuffer.flip();
                packetWritten = true;
            }

            if (socketBuffer.hasRemaining()) {
                int outputSize = cipher.getOutputSize(packetBuffer.remaining());
                if (outputSize <= socketBuffer.remaining()) {
                    cipher.update(packetBuffer, socketBuffer);
                } else {
                    int min = Math.min(packetBuffer.remaining(), socketBuffer.remaining());
                    int len = min / 2;
                    if (len > 0) {
                        int limitOld = packetBuffer.limit();
                        packetBuffer.limit(packetBuffer.position() + len);
                        cipher.update(packetBuffer, socketBuffer);
                        packetBuffer.limit(limitOld);
                    }
                }

                if (!packetBuffer.hasRemaining()) {
                    if (socketBuffer.remaining() >= cipher.getOutputSize(0)) {
                        socketBuffer.put(cipher.doFinal());
                        packetWritten = false;
                        packetBuffer.clear();
                        return true;
                    }
                }
            }
            return false;
        }
    }
}
