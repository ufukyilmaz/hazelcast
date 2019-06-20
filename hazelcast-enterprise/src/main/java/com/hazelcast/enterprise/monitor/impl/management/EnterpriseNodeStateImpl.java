package com.hazelcast.enterprise.monitor.impl.management;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.enterprise.monitor.LicenseInfo;
import com.hazelcast.enterprise.monitor.impl.rest.LicenseInfoImpl;
import com.hazelcast.instance.impl.NodeState;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.internal.json.JsonValue;
import com.hazelcast.monitor.impl.NodeStateImpl;
import com.hazelcast.version.MemberVersion;
import com.hazelcast.version.Version;

import java.util.List;
import java.util.Map;

public class EnterpriseNodeStateImpl
        extends NodeStateImpl {

    private LicenseInfo licenseInfo;

    public EnterpriseNodeStateImpl(ClusterState clusterState, NodeState nodeState, Version clusterVersion,
                                   MemberVersion memberVersion, LicenseInfo licenseInfo) {
        super(clusterState, nodeState, clusterVersion, memberVersion);
        this.licenseInfo = licenseInfo;
    }

    public EnterpriseNodeStateImpl(ClusterState clusterState, NodeState nodeState, Version clusterVersion,
                                   MemberVersion memberVersion, Map<String, List<String>> weakSecretsConfigs,
                                   LicenseInfo licenseInfo) {
        super(clusterState, nodeState, clusterVersion, memberVersion, weakSecretsConfigs);
        this.licenseInfo = licenseInfo;
    }

    @Override
    public JsonObject toJson() {
        JsonObject obj = super.toJson();
        if (licenseInfo != null) {
            obj.add("licenseInfo", licenseInfo.toJson());
        }

        return obj;
    }

    @Override
    public void fromJson(JsonObject json) {
        super.fromJson(json);
        JsonValue licenseInfoVal = json.get("licenseInfo");
        if (licenseInfo != null) {
            JsonObject licenseInfoObj = licenseInfoVal.asObject();
            LicenseInfo licenseInfo = new LicenseInfoImpl();
            licenseInfo.fromJson(licenseInfoObj);
            this.licenseInfo = licenseInfo;
        }
    }
}
