package com.hazelcast.enterprise;

import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.instance.BuildInfo;
import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.instance.Node;
import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;
import com.hazelcast.license.domain.License;
import com.hazelcast.license.util.LicenseHelper;
import com.hazelcast.logging.ILogger;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.util.MD5Util;
import com.hazelcast.util.PhoneHome;

import static com.hazelcast.cluster.memberselector.MemberSelectors.DATA_MEMBER_SELECTOR;

/**
 * Pings phone home server with cluster info daily.
 */
public class EnterprisePhoneHome extends PhoneHome {

    private final ILogger logger;
    private final BuildInfo buildInfo = BuildInfoProvider.getBuildInfo();

    public EnterprisePhoneHome(Node hazelcastNode) {
        super(hazelcastNode);
        logger = hazelcastNode.getLogger(EnterprisePhoneHome.class);
    }

    @Override
    public PhoneHomeParameterCreator createParameters(Node hazelcastNode) {
        //creates OS parameters first
        PhoneHomeParameterCreator parameters = super.createParameters(hazelcastNode);

        // calculate native memory usage from native memory config
        ClusterServiceImpl clusterService = hazelcastNode.getClusterService();
        NativeMemoryConfig memoryConfig = hazelcastNode.getConfig().getNativeMemoryConfig();
        long totalNativeMemorySize = clusterService.getSize(DATA_MEMBER_SELECTOR) * memoryConfig.getSize().bytes();
        String nativeMemoryParameter = Long.toString(MemoryUnit.BYTES.toGigaBytes(totalNativeMemorySize));

        final String licenseKey = hazelcastNode.getConfig().getLicenseKey();
        final String version = buildInfo.getVersion();
        License license = LicenseHelper.getLicense(licenseKey, version);
        final boolean isLicenseOEM = license.isOem();

        //add EE parameters
        parameters.addParam("e", "true");
        parameters.addParam("l", MD5Util.toMD5String(licenseKey));
        parameters.addParam("oem", Boolean.toString(isLicenseOEM));
        parameters.addParam("hdgb", nativeMemoryParameter);

        return parameters;
    }
}
