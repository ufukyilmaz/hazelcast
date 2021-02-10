/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.config;

import com.hazelcast.config.Config;
import com.hazelcast.internal.util.Preconditions;
import com.hazelcast.jet.core.JetProperties;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import javax.annotation.Nonnull;
import java.io.File;
import java.util.Properties;
import java.util.function.Consumer;

import static com.hazelcast.jet.core.JetProperties.JET_HOME;

/**
 * Configuration object for a Jet instance.
 *
 * @since 3.0
 */
public class JetConfig {

    /**
     * The default port number for the cluster auto-discovery mechanism's
     * multicast communication.
     */
    public static final int DEFAULT_JET_MULTICAST_PORT = 54326;

    /**
     * The default cluster name for a Jet cluster.
     */
    public static final String DEFAULT_CLUSTER_NAME = "jet";

    private static final ILogger LOGGER = Logger.getLogger(JetConfig.class);

    static {
        String value = jetHome();
        LOGGER.info("jet.home is " + value);
        System.setProperty(JET_HOME.getName(), value);
    }

    private Config hazelcastConfig = defaultHazelcastConfig();
    private InstanceConfig instanceConfig = new InstanceConfig();
    private EdgeConfig defaultEdgeConfig = new EdgeConfig();
    private Properties properties = new Properties();

    /**
     * Creates a new, empty {@code JetConfig} with the default configuration.
     * Doesn't consider any configuration XML files.
     */
    public JetConfig() {
    }


    /**
     * Returns the configuration object for the underlying Hazelcast IMDG
     * instance.
     */
    @Nonnull
    public Config getHazelcastConfig() {
        return hazelcastConfig;
    }


    /**
     * Convenience method for for configuring underlying Hazelcast IMDG instance.
     * Example:
     * <pre>{@code
     * JetConfig config = new JetConfig().configureHazelcast(c -> {
     *   c.getNetworkConfig().setPort(8000);
     *   c.setClusterName("jet-dev");
     * });
     * Jet.newJetInstance(config);
     * }</pre>
     */
    public JetConfig configureHazelcast(Consumer<Config> configConsumer) {
        configConsumer.accept(hazelcastConfig);
        return this;
    }

    /**
     * Sets the underlying Hazelcast IMDG instance's configuration object.
     */
    @Nonnull
    public JetConfig setHazelcastConfig(@Nonnull Config config) {
        Preconditions.checkNotNull(config, "config");
        hazelcastConfig = config;
        return this;
    }

    /**
     * Returns the Jet instance config.
     */
    @Nonnull
    public InstanceConfig getInstanceConfig() {
        return instanceConfig;
    }

    /**
     * Sets the Jet instance config.
     */
    @Nonnull
    public JetConfig setInstanceConfig(@Nonnull InstanceConfig instanceConfig) {
        Preconditions.checkNotNull(instanceConfig, "instanceConfig");
        this.instanceConfig = instanceConfig;
        return this;
    }

    /**
     * Returns the Jet-specific configuration properties.
     */
    @Nonnull
    public Properties getProperties() {
        return properties;
    }

    /**
     * Sets the Jet-specific configuration properties.
     */
    @Nonnull
    public JetConfig setProperties(@Nonnull Properties properties) {
        Preconditions.checkNotNull(properties, "properties");
        this.properties = properties;
        return this;
    }

    /**
     * Sets the value of the specified property.
     */
    @Nonnull
    public JetConfig setProperty(@Nonnull String name, @Nonnull String value) {
        Preconditions.checkNotNull(name, "name");
        Preconditions.checkNotNull(value, "value");
        properties.put(name, value);
        return this;
    }

    /**
     * Returns the default DAG edge configuration.
     */
    @Nonnull
    public EdgeConfig getDefaultEdgeConfig() {
        return defaultEdgeConfig;
    }

    /**
     * Sets the configuration object that specifies the defaults to use
     * for a DAG edge configuration.
     */
    @Nonnull
    public JetConfig setDefaultEdgeConfig(@Nonnull EdgeConfig defaultEdgeConfig) {
        Preconditions.checkNotNull(defaultEdgeConfig, "defaultEdgeConfig");
        this.defaultEdgeConfig = defaultEdgeConfig;
        return this;
    }

    private static Config defaultHazelcastConfig() {
        Config config = new Config();
        config.getNetworkConfig().getJoin().getMulticastConfig().setMulticastPort(DEFAULT_JET_MULTICAST_PORT);
        config.setClusterName(DEFAULT_CLUSTER_NAME);
        config.getHotRestartPersistenceConfig().setBaseDir(new File(jetHome(), "recovery").getAbsoluteFile());
        return config;
    }

    /**
     * Returns the absolute path for jet.home based from the system property
     * {@link JetProperties#JET_HOME}
     */
    private static String jetHome() {
        return new File(System.getProperty(JET_HOME.getName(), JET_HOME.getDefaultValue())).getAbsolutePath();
    }
}
