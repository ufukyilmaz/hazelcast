/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.clientside;

import com.hazelcast.client.ClientExtension;
import com.hazelcast.client.config.ClientAliasedDiscoveryConfigUtils;
import com.hazelcast.client.config.ClientCloudConfig;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.client.config.ClientSecurityConfig;
import com.hazelcast.client.config.SocketOptions;
import com.hazelcast.client.connection.AddressProvider;
import com.hazelcast.client.connection.AddressTranslator;
import com.hazelcast.client.connection.nio.DefaultCredentialsFactory;
import com.hazelcast.client.spi.impl.DefaultAddressProvider;
import com.hazelcast.client.spi.impl.DefaultAddressTranslator;
import com.hazelcast.client.spi.impl.discovery.DiscoveryAddressProvider;
import com.hazelcast.client.spi.impl.discovery.DiscoveryAddressTranslator;
import com.hazelcast.client.spi.impl.discovery.HazelcastCloudAddressProvider;
import com.hazelcast.client.spi.impl.discovery.HazelcastCloudAddressTranslator;
import com.hazelcast.client.spi.impl.discovery.HazelcastCloudDiscovery;
import com.hazelcast.client.spi.properties.ClientProperty;
import com.hazelcast.config.CredentialsFactoryConfig;
import com.hazelcast.config.DiscoveryConfig;
import com.hazelcast.config.DiscoveryStrategyConfig;
import com.hazelcast.config.SSLConfig;
import com.hazelcast.config.SocketInterceptorConfig;
import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.internal.networking.ChannelInitializer;
import com.hazelcast.internal.networking.ChannelInitializerProvider;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.nio.ClassLoaderUtil;
import com.hazelcast.nio.SocketInterceptor;
import com.hazelcast.security.ICredentialsFactory;
import com.hazelcast.spi.discovery.impl.DefaultDiscoveryServiceProvider;
import com.hazelcast.spi.discovery.integration.DiscoveryMode;
import com.hazelcast.spi.discovery.integration.DiscoveryService;
import com.hazelcast.spi.discovery.integration.DiscoveryServiceProvider;
import com.hazelcast.spi.discovery.integration.DiscoveryServiceSettings;
import com.hazelcast.spi.properties.HazelcastProperties;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static com.hazelcast.client.spi.properties.ClientProperty.DISCOVERY_SPI_ENABLED;
import static com.hazelcast.client.spi.properties.ClientProperty.HAZELCAST_CLOUD_DISCOVERY_TOKEN;
import static com.hazelcast.config.AliasedDiscoveryConfigUtils.allUsePublicAddress;
import static com.hazelcast.util.ExceptionUtil.rethrow;

class ClientDiscoveryServiceBuilder {

    private final LoggingService loggingService;
    private final AddressProvider externalAddressProvider;
    private final HazelcastProperties properties;
    private final ClientExtension clientExtension;
    private final Collection<ClientConfig> configs;
    private final int configsTryCount;

    ClientDiscoveryServiceBuilder(int configsTryCount, List<ClientConfig> configs, LoggingService loggingService,
                                  AddressProvider externalAddressProvider, HazelcastProperties properties,
                                  ClientExtension clientExtension) {
        this.configsTryCount = configsTryCount;
        this.configs = configs;
        this.loggingService = loggingService;
        this.externalAddressProvider = externalAddressProvider;
        this.properties = properties;
        this.clientExtension = clientExtension;
    }

    public ClientDiscoveryService build() {
        ArrayList<CandidateClusterContext> contexts = new ArrayList<CandidateClusterContext>();
        for (ClientConfig config : configs) {
            ClientNetworkConfig networkConfig = config.getNetworkConfig();
            SocketInterceptor interceptor = initSocketInterceptor(networkConfig.getSocketInterceptorConfig());
            ICredentialsFactory credentialsFactory = initCredentialsFactory(config);
            DiscoveryService discoveryService = initDiscoveryService(config);
            AddressProvider provider = createAddressProvider(config, discoveryService, externalAddressProvider);
            AddressTranslator translator = createAddressTranslator(config, discoveryService);
            final SSLConfig sslConfig = networkConfig.getSSLConfig();
            final SocketOptions socketOptions = networkConfig.getSocketOptions();
            contexts.add(new CandidateClusterContext(provider, translator, discoveryService,
                    credentialsFactory, interceptor, new ChannelInitializerProvider() {
                @Override
                public ChannelInitializer provide(EndpointQualifier qualifier) {
                    return clientExtension.createChannelInitializer(sslConfig, socketOptions);
                }
            }));
        }
        return new ClientDiscoveryService(configsTryCount, contexts);
    }

    private AddressProvider createAddressProvider(ClientConfig clientConfig,
                                                  DiscoveryService discoveryService, AddressProvider externalAddressProvider) {
        ClientNetworkConfig networkConfig = clientConfig.getNetworkConfig();


        if (externalAddressProvider != null) {
            return externalAddressProvider;
        }

        if (discoveryService != null) {
            return new DiscoveryAddressProvider(discoveryService);
        }

        HazelcastCloudAddressProvider cloudAddressProvider = initCloudAddressProvider(clientConfig);
        if (cloudAddressProvider != null) {
            return cloudAddressProvider;
        }

        return new DefaultAddressProvider(networkConfig);
    }

    private HazelcastCloudAddressProvider initCloudAddressProvider(ClientConfig clientConfig) {
        ClientNetworkConfig networkConfig = clientConfig.getNetworkConfig();
        ClientCloudConfig cloudConfig = networkConfig.getCloudConfig();

        if (cloudConfig.isEnabled()) {
            String discoveryToken = cloudConfig.getDiscoveryToken();
            String cloudUrlBase = properties.getString(HazelcastCloudDiscovery.CLOUD_URL_BASE_PROPERTY);
            String urlEndpoint = HazelcastCloudDiscovery.createUrlEndpoint(cloudUrlBase, discoveryToken);
            return new HazelcastCloudAddressProvider(urlEndpoint, getConnectionTimeoutMillis(networkConfig), loggingService);
        }

        String cloudToken = properties.getString(ClientProperty.HAZELCAST_CLOUD_DISCOVERY_TOKEN);
        if (cloudToken != null) {
            String cloudUrlBase = properties.getString(HazelcastCloudDiscovery.CLOUD_URL_BASE_PROPERTY);
            String urlEndpoint = HazelcastCloudDiscovery.createUrlEndpoint(cloudUrlBase, cloudToken);
            return new HazelcastCloudAddressProvider(urlEndpoint, getConnectionTimeoutMillis(networkConfig), loggingService);
        }
        return null;
    }

    private AddressTranslator createAddressTranslator(ClientConfig clientConfig, DiscoveryService discoveryService) {
        ClientNetworkConfig networkConfig = clientConfig.getNetworkConfig();
        ClientCloudConfig cloudConfig = networkConfig.getCloudConfig();

        List<String> addresses = networkConfig.getAddresses();
        boolean addressListProvided = addresses.size() != 0;
        boolean awsDiscoveryEnabled = networkConfig.getAwsConfig() != null && networkConfig.getAwsConfig().isEnabled();
        boolean gcpDiscoveryEnabled = networkConfig.getGcpConfig() != null && networkConfig.getGcpConfig().isEnabled();
        boolean azureDiscoveryEnabled = networkConfig.getAzureConfig() != null && networkConfig.getAzureConfig().isEnabled();
        boolean kubernetesDiscoveryEnabled = networkConfig.getKubernetesConfig() != null
                && networkConfig.getKubernetesConfig().isEnabled();
        boolean eurekaDiscoveryEnabled = networkConfig.getEurekaConfig() != null && networkConfig.getEurekaConfig().isEnabled();
        boolean discoverySpiEnabled = discoverySpiEnabled(networkConfig);
        String cloudDiscoveryToken = properties.getString(HAZELCAST_CLOUD_DISCOVERY_TOKEN);
        if (cloudDiscoveryToken != null && cloudConfig.isEnabled()) {
            throw new IllegalStateException("Ambiguous hazelcast.cloud configuration. "
                    + "Both property based and client configuration based settings are provided for "
                    + "Hazelcast cloud discovery together. Use only one.");
        }
        boolean hazelcastCloudEnabled = cloudDiscoveryToken != null || cloudConfig.isEnabled();
        isDiscoveryConfigurationConsistent(addressListProvided, awsDiscoveryEnabled, gcpDiscoveryEnabled, azureDiscoveryEnabled,
                kubernetesDiscoveryEnabled, eurekaDiscoveryEnabled, discoverySpiEnabled, hazelcastCloudEnabled);

        if (discoveryService != null) {
            return new DiscoveryAddressTranslator(discoveryService, usePublicAddress(clientConfig));
        } else if (hazelcastCloudEnabled) {
            String discoveryToken;
            if (cloudConfig.isEnabled()) {
                discoveryToken = cloudConfig.getDiscoveryToken();
            } else {
                discoveryToken = cloudDiscoveryToken;
            }
            String cloudUrlBase = properties.getString(HazelcastCloudDiscovery.CLOUD_URL_BASE_PROPERTY);
            String urlEndpoint = HazelcastCloudDiscovery.createUrlEndpoint(cloudUrlBase, discoveryToken);
            return new HazelcastCloudAddressTranslator(urlEndpoint, getConnectionTimeoutMillis(networkConfig), loggingService);
        }

        return new DefaultAddressTranslator();
    }

    private boolean discoverySpiEnabled(ClientNetworkConfig networkConfig) {
        return (networkConfig.getDiscoveryConfig() != null && networkConfig.getDiscoveryConfig().isEnabled())
                || Boolean.parseBoolean(properties.getString(DISCOVERY_SPI_ENABLED));
    }

    private boolean usePublicAddress(ClientConfig config) {
        return properties.getBoolean(ClientProperty.DISCOVERY_SPI_PUBLIC_IP_ENABLED)
                || allUsePublicAddress(ClientAliasedDiscoveryConfigUtils.aliasedDiscoveryConfigsFrom(config.getNetworkConfig()));
    }

    @SuppressWarnings({"checkstyle:booleanexpressioncomplexity", "checkstyle:npathcomplexity"})
    private void isDiscoveryConfigurationConsistent(boolean addressListProvided, boolean awsDiscoveryEnabled,
                                                    boolean gcpDiscoveryEnabled, boolean azureDiscoveryEnabled,
                                                    boolean kubernetesDiscoveryEnabled, boolean eurekaDiscoveryEnabled,
                                                    boolean discoverySpiEnabled, boolean hazelcastCloudEnabled) {
        int count = 0;
        if (addressListProvided) {
            count++;
        }
        if (awsDiscoveryEnabled) {
            count++;
        }
        if (gcpDiscoveryEnabled) {
            count++;
        }
        if (azureDiscoveryEnabled) {
            count++;
        }
        if (kubernetesDiscoveryEnabled) {
            count++;
        }
        if (eurekaDiscoveryEnabled) {
            count++;
        }
        if (discoverySpiEnabled) {
            count++;
        }
        if (hazelcastCloudEnabled) {
            count++;
        }
        if (count > 1) {
            throw new IllegalStateException("Only one discovery method can be enabled at a time. "
                    + "cluster members given explicitly : " + addressListProvided
                    + ", aws discovery: " + awsDiscoveryEnabled
                    + ", gcp discovery: " + gcpDiscoveryEnabled
                    + ", azure discovery: " + azureDiscoveryEnabled
                    + ", kubernetes discovery: " + kubernetesDiscoveryEnabled
                    + ", eureka discovery: " + eurekaDiscoveryEnabled
                    + ", discovery spi enabled : " + discoverySpiEnabled
                    + ", hazelcast.cloud enabled : " + hazelcastCloudEnabled);
        }
    }

    private DiscoveryService initDiscoveryService(ClientConfig config) {
        // Prevent confusing behavior where the DiscoveryService is started
        // and strategies are resolved but the AddressProvider is never registered
        List<DiscoveryStrategyConfig> aliasedDiscoveryConfigs =
                ClientAliasedDiscoveryConfigUtils.createDiscoveryStrategyConfigs(config);

        if (!properties.getBoolean(ClientProperty.DISCOVERY_SPI_ENABLED) && aliasedDiscoveryConfigs.isEmpty()) {
            return null;
        }

        ILogger logger = loggingService.getLogger(DiscoveryService.class);
        ClientNetworkConfig networkConfig = config.getNetworkConfig();
        DiscoveryConfig discoveryConfig = networkConfig.getDiscoveryConfig().getAsReadOnly();

        DiscoveryServiceProvider factory = discoveryConfig.getDiscoveryServiceProvider();
        if (factory == null) {
            factory = new DefaultDiscoveryServiceProvider();
        }

        DiscoveryServiceSettings settings = new DiscoveryServiceSettings()
                .setConfigClassLoader(config.getClassLoader())
                .setLogger(logger)
                .setDiscoveryMode(DiscoveryMode.Client)
                .setAliasedDiscoveryConfigs(aliasedDiscoveryConfigs)
                .setDiscoveryConfig(discoveryConfig);

        DiscoveryService discoveryService = factory.newDiscoveryService(settings);
        discoveryService.start();
        return discoveryService;
    }

    private ICredentialsFactory initCredentialsFactory(ClientConfig config) {
        ClientSecurityConfig securityConfig = config.getSecurityConfig();
        validateSecurityConfig(securityConfig);
        ICredentialsFactory c = getCredentialsFromFactory(config);
        if (c == null) {
            return new DefaultCredentialsFactory(securityConfig, config.getGroupConfig(), config.getClassLoader());
        }
        return c;
    }

    private void validateSecurityConfig(ClientSecurityConfig securityConfig) {
        boolean configuredViaCredentials = securityConfig.getCredentials() != null
                || securityConfig.getCredentialsClassname() != null;

        CredentialsFactoryConfig factoryConfig = securityConfig.getCredentialsFactoryConfig();
        boolean configuredViaCredentialsFactory = factoryConfig.getClassName() != null
                || factoryConfig.getImplementation() != null;

        if (configuredViaCredentials && configuredViaCredentialsFactory) {
            throw new IllegalStateException("Ambiguous Credentials config. Set only one of Credentials or ICredentialsFactory");
        }
    }

    private ICredentialsFactory getCredentialsFromFactory(ClientConfig config) {
        CredentialsFactoryConfig credentialsFactoryConfig = config.getSecurityConfig().getCredentialsFactoryConfig();
        ICredentialsFactory factory = credentialsFactoryConfig.getImplementation();
        if (factory == null) {
            String factoryClassName = credentialsFactoryConfig.getClassName();
            if (factoryClassName != null) {
                try {
                    factory = ClassLoaderUtil.newInstance(config.getClassLoader(), factoryClassName);
                } catch (Exception e) {
                    throw rethrow(e);
                }
            }
        }
        if (factory == null) {
            return null;
        }
        factory.configure(config.getGroupConfig(), credentialsFactoryConfig.getProperties());
        return factory;
    }

    private SocketInterceptor initSocketInterceptor(SocketInterceptorConfig sic) {
        if (sic != null && sic.isEnabled()) {
            return clientExtension.createSocketInterceptor(sic);
        }
        return null;
    }

    private int getConnectionTimeoutMillis(ClientNetworkConfig networkConfig) {
        int connTimeout = networkConfig.getConnectionTimeout();
        return connTimeout == 0 ? Integer.MAX_VALUE : connTimeout;
    }
}
