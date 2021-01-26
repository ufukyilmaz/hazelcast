package com.hazelcast.internal;

import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.instance.impl.EnterpriseNodeExtension;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;
import com.hazelcast.internal.util.MD5Util;
import com.hazelcast.internal.util.phonehome.PhoneHome;
import com.hazelcast.internal.util.phonehome.PhoneHomeParameterCreator;
import com.hazelcast.license.domain.License;
import com.hazelcast.memory.MemoryUnit;

import static com.hazelcast.cluster.memberselector.MemberSelectors.DATA_MEMBER_SELECTOR;

/**
 * Pings phone home server with cluster info daily.
 */
public class EnterprisePhoneHome extends PhoneHome {

    public EnterprisePhoneHome(Node hazelcastNode) {
        super(hazelcastNode);
    }

    @Override
    public PhoneHomeParameterCreator createParameters() {
        //creates OS parameters first
        PhoneHomeParameterCreator parameters = super.createParameters();

        // calculate native memory usage from native memory config
        ClusterServiceImpl clusterService = hazelcastNode.getClusterService();
        NativeMemoryConfig memoryConfig = hazelcastNode.getConfig().getNativeMemoryConfig();
        long totalNativeMemorySize = clusterService.getSize(DATA_MEMBER_SELECTOR) * memoryConfig.getSize().bytes();
        String nativeMemoryParameter = Long.toString(MemoryUnit.BYTES.toGigaBytes(totalNativeMemorySize));

        EnterpriseNodeExtension nodeExtension = (EnterpriseNodeExtension) hazelcastNode.getNodeExtension();
        License license = nodeExtension.getLicense();
        final String licenseKey = license.getKey();
        final String keyHash = licenseKey != null ? MD5Util.toMD5String(licenseKey) : "";
        final boolean isLicenseOEM = license.isOem();

        //add EE parameters
        parameters.addParam("e", "true");
        parameters.addParam("l", keyHash);
        parameters.addParam("oem", Boolean.toString(isLicenseOEM));
        parameters.addParam("hdgb", nativeMemoryParameter);

        return parameters;
    }
}
