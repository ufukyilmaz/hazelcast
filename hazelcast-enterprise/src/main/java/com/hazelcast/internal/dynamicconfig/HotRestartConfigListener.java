package com.hazelcast.internal.dynamicconfig;

import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.internal.hotrestart.HotRestartIntegrationService;
import com.hazelcast.internal.hotrestart.LoadedConfigurationListener;

/**
 * Persist dynamic configurations into HotRestart when enabled
 *
 */
public class HotRestartConfigListener implements DynamicConfigListener {
    private final HotRestartIntegrationService hotRestartIntegrationService;

    public HotRestartConfigListener(HotRestartIntegrationService hotRestartIntegrationService) {
        this.hotRestartIntegrationService = hotRestartIntegrationService;
    }


    @Override
    public void onServiceInitialized(final ClusterWideConfigurationService configurationService) {
        hotRestartIntegrationService.registerLoadedConfigurationListener(new LoadedConfigurationListener() {
            @Override
            public void onConfigurationLoaded(String serviceName, String name, Object config) {
                if (MapService.SERVICE_NAME.equals(serviceName)) {
                    MapConfig mapConfig = (MapConfig) config;
                    configurationService.registerConfigLocally(mapConfig, ConfigCheckMode.WARNING);
                }
            }
        });
    }

    @Override
    public void onConfigRegistered(MapConfig configObject) {
        if (configObject.getHotRestartConfig().isEnabled()) {
            hotRestartIntegrationService.ensureHasConfiguration(MapService.SERVICE_NAME, configObject.getName(), configObject);
        }
    }

    @Override
    public void onConfigRegistered(CacheSimpleConfig configObject) {
        if (configObject.getHotRestartConfig().isEnabled()) {
            hotRestartIntegrationService.ensureHasConfiguration(CacheService.SERVICE_NAME, configObject.getName(), configObject);
        }
    }
}
