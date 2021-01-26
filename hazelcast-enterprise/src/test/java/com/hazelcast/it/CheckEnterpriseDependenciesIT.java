package com.hazelcast.it;

import com.hazelcast.osgi.CheckDependenciesIT;

public class CheckEnterpriseDependenciesIT extends CheckDependenciesIT {

    @Override
    protected boolean isMatching(String urlString) {
        return urlString.contains("hazelcast-enterprise-" + getMajorVersion() + ".") && urlString.contains("target");
    }

    @Override
    protected String getBundleName() {
        return "Hazelcast Enterprise(Core)";
    }
}
