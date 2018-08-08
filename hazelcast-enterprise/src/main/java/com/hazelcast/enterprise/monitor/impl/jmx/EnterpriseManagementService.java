package com.hazelcast.enterprise.monitor.impl.jmx;

import com.hazelcast.enterprise.monitor.LicenseInfo;
import com.hazelcast.enterprise.monitor.impl.rest.LicenseInfoImpl;
import com.hazelcast.instance.HazelcastInstanceImpl;
import com.hazelcast.internal.jmx.InstanceMBean;
import com.hazelcast.internal.jmx.ManagementService;
import com.hazelcast.license.util.LicenseHelper;

/**
 * JMX Extention point for EE
 * Offers additional JMX beans to the service for the Enterprise version of Hazelcast
 */
public class EnterpriseManagementService
        extends ManagementService {

    public EnterpriseManagementService(HazelcastInstanceImpl instance) {
        super(instance);
    }

    @Override
    protected InstanceMBean createInstanceMBean(HazelcastInstanceImpl instance) {
        return new EnterpriseInstanceMBean(instance, this);
    }

    private static class EnterpriseInstanceMBean
            extends InstanceMBean {

        private LicenseInfoMBean licenseInfoMBean;

        EnterpriseInstanceMBean(HazelcastInstanceImpl hazelcastInstance, ManagementService managementService) {
            super(hazelcastInstance, managementService);
            createAndRegisterLicenseMBean(hazelcastInstance, managementService);
        }

        private void createAndRegisterLicenseMBean(HazelcastInstanceImpl hazelcastInstance, ManagementService managementService) {
            LicenseInfo licenseInfo = new LicenseInfoImpl(LicenseHelper.getLicense(
                    hazelcastInstance.node.getConfig().getLicenseKey(), hazelcastInstance.node.getBuildInfo().getVersion()));
            this.licenseInfoMBean = new LicenseInfoMBean(licenseInfo, hazelcastInstance.node, managementService);
            register(licenseInfoMBean);
        }

    }

}
