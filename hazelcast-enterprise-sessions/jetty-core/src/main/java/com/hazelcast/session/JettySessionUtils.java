package com.hazelcast.session;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.XmlClientConfigBuilder;
import com.hazelcast.config.Config;
import com.hazelcast.config.ConfigLoader;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.BuildInfo;
import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.instance.GroupProperty;
import com.hazelcast.license.domain.LicenseType;
import com.hazelcast.license.util.LicenseHelper;

import java.io.IOException;

import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * Utility class for Jetty Session Replication modules
 */
public final class JettySessionUtils {

    public static final String DEFAULT_INSTANCE_NAME = "SESSION-REPLICATION-INSTANCE";
    public static final String DEFAULT_MAP_NAME = "session-replication-map";

    public static final int DAY_IN_MILLISECONDS = 24 * 60 * 60 * 1000;
    public static final int HOUR_IN_MILLISECONDS = 60 * 60 * 1000;

    private JettySessionUtils() {
    }

    /**
     * Create a Hazelcast client instance to connect an existing cluster
     */
    public static HazelcastInstance createHazelcastClientInstance(String configLocation) {
        ClientConfig config;
        try {
            XmlClientConfigBuilder builder;
            if (configLocation == null) {
                builder = new XmlClientConfigBuilder();
            } else {
                builder = new XmlClientConfigBuilder(configLocation);
            }
            config = builder.build();
            String licenseKey = config.getLicenseKey();
            if (licenseKey == null) {
                licenseKey = config.getProperty(GroupProperty.ENTERPRISE_LICENSE_KEY);
            }
            BuildInfo buildInfo = BuildInfoProvider.getBuildInfo();
            LicenseHelper.checkLicenseKey(licenseKey, buildInfo.getVersion(),
                    LicenseType.ENTERPRISE, LicenseType.ENTERPRISE_HD);
        } catch (IOException e) {
            throw new RuntimeException("failed to load config", e);
        }

        checkNotNull(config, "failed to find configLocation: " + configLocation);

        return HazelcastClient.newHazelcastClient(config);
    }

    public static HazelcastInstance createHazelcastFullInstance(String configLocation) {
        Config config;
        try {
            if (configLocation == null) {
                config = new XmlConfigBuilder().build();
            } else {
                config = ConfigLoader.load(configLocation);
            }
            String licenseKey = config.getLicenseKey();

            BuildInfo buildInfo = BuildInfoProvider.getBuildInfo();
            LicenseHelper.checkLicenseKey(licenseKey, buildInfo.getVersion(), LicenseType.ENTERPRISE);
        } catch (IOException e) {
            throw new RuntimeException("failed to load config", e);
        }

        checkNotNull(config, "failed to find configLocation: " + configLocation);

        config.setInstanceName(DEFAULT_INSTANCE_NAME);
        return Hazelcast.getOrCreateHazelcastInstance(config);
    }
}
