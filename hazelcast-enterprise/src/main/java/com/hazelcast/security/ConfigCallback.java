package com.hazelcast.security;

import javax.security.auth.callback.Callback;

import com.hazelcast.config.Config;

/**
 * This JAAS {@link Callback} is used to retrieve member {@link Config}.
 * It can be passed to {@link com.hazelcast.security.impl.ClusterCallbackHandler}
 * and used by {@link javax.security.auth.spi.LoginModule LoginModules}
 * during login process.
 */
public class ConfigCallback implements Callback {

    private Config config;

    public void setConfig(Config config) {
        this.config = config;
    }

    public Config getConfig() {
        return config;
    }
}
