package com.hazelcast.it;

import com.hazelcast.osgi.CheckDependenciesIT;

public class CheckEnterpriseAllDependenciesIT extends CheckDependenciesIT {

    @Override
    protected boolean isMatching(String urlString) {
        return urlString.contains("hazelcast-enterprise-all-") && urlString.contains("target");
    }

    @Override
    protected String getBundleName() {
        return "Hazelcast Enterprise(All)";
    }
}
