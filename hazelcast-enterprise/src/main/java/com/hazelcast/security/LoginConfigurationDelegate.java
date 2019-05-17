package com.hazelcast.security;

import com.hazelcast.config.LoginModuleConfig;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.AppConfigurationEntry.LoginModuleControlFlag;
import javax.security.auth.login.Configuration;
import java.util.HashMap;
import java.util.Map;

public final class LoginConfigurationDelegate extends Configuration {

    private final LoginModuleConfig[] loginModuleConfigs;

    @SuppressFBWarnings("EI_EXPOSE_REP2")
    public LoginConfigurationDelegate(LoginModuleConfig[] loginModuleConfigs) {
        super();
        this.loginModuleConfigs = loginModuleConfigs;
    }

    @Override
    public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
        final AppConfigurationEntry[] entries = new AppConfigurationEntry[loginModuleConfigs.length];
        for (int i = 0; i < loginModuleConfigs.length; i++) {
            final LoginModuleConfig module = loginModuleConfigs[i];
            LoginModuleControlFlag flag = getFlag(module);
            final Map options = new HashMap(module.getProperties());
            entries[i] = new AppConfigurationEntry(module.getClassName(), flag, options);
        }
        return entries;
    }

    private LoginModuleControlFlag getFlag(LoginModuleConfig module) {
        switch (module.getUsage()) {
            case REQUIRED:
                return LoginModuleControlFlag.REQUIRED;

            case OPTIONAL:
                return LoginModuleControlFlag.OPTIONAL;

            case REQUISITE:
                return LoginModuleControlFlag.REQUISITE;

            case SUFFICIENT:
                return LoginModuleControlFlag.SUFFICIENT;

            default:
                return LoginModuleControlFlag.REQUIRED;
        }
    }

    @Override
    public void refresh() {
    }
}
