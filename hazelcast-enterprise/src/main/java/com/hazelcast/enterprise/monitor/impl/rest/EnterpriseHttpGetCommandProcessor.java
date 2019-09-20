package com.hazelcast.enterprise.monitor.impl.rest;

import com.hazelcast.enterprise.monitor.LicenseInfo;
import com.hazelcast.instance.impl.EnterpriseNodeExtension;
import com.hazelcast.internal.ascii.TextCommandService;
import com.hazelcast.internal.ascii.rest.HttpGetCommand;
import com.hazelcast.internal.ascii.rest.HttpGetCommandProcessor;

import static com.hazelcast.internal.ascii.TextCommandConstants.MIME_TEXT_PLAIN;
import static com.hazelcast.internal.util.StringUtil.stringToBytes;

/**
 * Enterprise override for HTTP GET command processor. It handles the license info requests.
 */
class EnterpriseHttpGetCommandProcessor
        extends HttpGetCommandProcessor {

    EnterpriseHttpGetCommandProcessor(TextCommandService textCommandService) {
        super(textCommandService);
    }

    protected void handleLicense(HttpGetCommand command) {
        EnterpriseNodeExtension nodeExtension = (EnterpriseNodeExtension) textCommandService.getNode().getNodeExtension();
        LicenseInfo licenseInfo = new LicenseInfoImpl(nodeExtension.getLicense());

        StringBuilder res = new StringBuilder();
        res.append("licenseInfo").append(licenseInfo.toJson()).append("\n");
        command.setResponse(MIME_TEXT_PLAIN, stringToBytes(res.toString()));
    }
}
