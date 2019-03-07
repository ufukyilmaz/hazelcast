package com.hazelcast.security.impl;

import com.hazelcast.config.GroupConfig;
import com.hazelcast.security.Credentials;
import com.hazelcast.security.ICredentialsFactory;
import com.hazelcast.security.UsernamePasswordCredentials;

import java.util.Properties;

/**
 * The default {@link ICredentialsFactory}.
 *
 * This class is not unused, it's set via {@link com.hazelcast.security.SecurityConstants#DEFAULT_CREDENTIALS_FACTORY_CLASS}.
 */
@SuppressWarnings("unused")
public class DefaultCredentialsFactory implements ICredentialsFactory {

    private Credentials credentials;

    @Override
    public void configure(GroupConfig groupConfig, Properties properties) {
        credentials = new UsernamePasswordCredentials(groupConfig.getName(), groupConfig.getPassword());
    }

    @Override
    public Credentials newCredentials() {
        return credentials;
    }

    @Override
    public void destroy() {
    }
}