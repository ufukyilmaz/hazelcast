package com.hazelcast.enterprise.monitor.impl.rest;

import com.hazelcast.enterprise.monitor.LicenseInfo;
import com.hazelcast.instance.EnterpriseNodeExtension;
import com.hazelcast.internal.ascii.TextCommandService;
import com.hazelcast.internal.ascii.rest.HttpPostCommandProcessor;

/**
 * Enterprise override for HTTP POST command processor.
 */
class EnterpriseHttpPostCommandProcessor
        extends HttpPostCommandProcessor {

    EnterpriseHttpPostCommandProcessor(TextCommandService textCommandService) {
        super(textCommandService);
    }

    @Override
    protected String responseOnSetLicenseSuccess() {
        EnterpriseNodeExtension nodeExtension = (EnterpriseNodeExtension) textCommandService.getNode().getNodeExtension();
        LicenseInfo licenseInfo = new LicenseInfoImpl(nodeExtension.getLicense());
        return response(ResponseType.SUCCESS, "licenseInfo", licenseInfo.toJson(), "message",
                "License updated at run time - please make sure to update the license in the persistent"
                        + " configuration to avoid losing the changes on restart.");
    }

}
