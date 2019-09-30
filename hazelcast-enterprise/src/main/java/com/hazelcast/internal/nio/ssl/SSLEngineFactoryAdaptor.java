package com.hazelcast.internal.nio.ssl;

import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.ssl.SSLContextFactory;
import com.hazelcast.nio.ssl.SSLEngineFactory;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static com.hazelcast.internal.nio.ssl.SSLEngineFactorySupport.getProperty;
import static com.hazelcast.internal.util.StringUtil.intersection;
import static com.hazelcast.internal.util.StringUtil.splitByComma;

/**
 * An {@link SSLEngineFactory} that adapts a {@link SSLContextFactory} to act like a {@link SSLEngineFactory}.
 */
public class SSLEngineFactoryAdaptor implements SSLEngineFactory {

    private final ILogger logger = Logger.getLogger(SSLEngineFactoryAdaptor.class);

    private final SSLContextFactory sslContextFactory;
    private volatile String[] cipherSuites;
    private volatile String protocol;

    public SSLEngineFactoryAdaptor(SSLContextFactory sslContextFactory) {
        this.sslContextFactory = sslContextFactory;
    }

    @Override
    public SSLEngine create(boolean clientMode) {
        SSLContext sslContext = sslContextFactory.getSSLContext();
        SSLEngine sslEngine = sslContext.createSSLEngine();
        sslEngine.setUseClientMode(clientMode);
        sslEngine.setEnableSessionCreation(true);
        if (cipherSuites != null) {
            sslEngine.setEnabledCipherSuites(cipherSuites);
        }
        if (protocol != null) {
            String[] enabledProtocols = findEnabledProtocols(protocol, sslEngine.getSupportedProtocols());
            if (enabledProtocols.length > 0) {
                sslEngine.setEnabledProtocols(enabledProtocols);
            } else {
                // log waring, but allow to continue - backward compatible behavior
                logger.warning("Enabling SSL protocol failed. Check if configured value contains a supported value"
                        + Arrays.toString(sslEngine.getSupportedProtocols()));
            }
        }
        return sslEngine;
    }

    @Override
    public void init(Properties properties, boolean forClient) throws Exception {
        sslContextFactory.init(properties);
        String[] configuredCipherSuites = splitByComma(getProperty(properties, "ciphersuites"), false);
        if (configuredCipherSuites != null) {
            // force using configured cipher suites
            SSLEngine sslEngine = sslContextFactory.getSSLContext().createSSLEngine();
            String[] supportedCipherSuites = sslEngine.getSupportedCipherSuites();
            // find intersection between configured and supported ciphersuites
            cipherSuites = intersection(configuredCipherSuites, supportedCipherSuites);
            // fail fast if no valid ciphersuite found
            if (cipherSuites.length < 1) {
                throw new InvalidConfigurationException("No configured SSL cipher suite name is valid. "
                        + "Check if configured values "
                        + Arrays.toString(configuredCipherSuites) + " contain supported values: "
                        + Arrays.toString(supportedCipherSuites));
            }
        }
        this.protocol = getProperty(properties, "protocol");
    }

    /**
     * Finds supported protocol names for given configured protocol property.
     *
     * @param configuredName     value of configured protocol property
     * @param supportedProtocols protocols supported by theSSL engine
     * @return supported protocols which match given configured name
     */
    private String[] findEnabledProtocols(String configuredName, String[] supportedProtocols) {
        List<String> enabled = new ArrayList<String>();
        for (String protocol : supportedProtocols) {
            if (configuredName.equals(protocol) || ("TLS".equals(configuredName) && protocol.matches("TLSv1(\\.\\d+)?"))
                    || ("SSL".equals(configuredName) && protocol.equals("SSLv3"))) {
                enabled.add(protocol);
            }
        }
        return enabled.toArray(new String[enabled.size()]);
    }
}
