package com.hazelcast.enterprise.monitor.impl.rest;

import com.hazelcast.enterprise.monitor.LicenseInfo;
import com.hazelcast.instance.Node;
import com.hazelcast.internal.ascii.TextCommandService;
import com.hazelcast.internal.ascii.rest.HttpGetCommand;
import com.hazelcast.internal.ascii.rest.HttpGetCommandProcessor;
import com.hazelcast.license.util.LicenseHelper;

import static com.hazelcast.internal.ascii.TextCommandConstants.MIME_TEXT_PLAIN;
import static com.hazelcast.util.StringUtil.stringToBytes;

/**
 * Enterprise override for HTTP GET command processor. It handles the license info requests.
 */
class EnterpriseHttpGetCommandProcessor
        extends HttpGetCommandProcessor {

    EnterpriseHttpGetCommandProcessor(TextCommandService textCommandService) {
        super(textCommandService);
    }

    @Override
    protected void handleLicense(HttpGetCommand command) {
        Node node = textCommandService.getNode();
        LicenseInfo licenseInfo = new LicenseInfoImpl(LicenseHelper.getLicense(
                node.getConfig().getLicenseKey(), node.getBuildInfo().getVersion()));

        StringBuilder res = new StringBuilder();
        res.append("licenseInfo").append(licenseInfo.toJson()).append("\n");
        command.setResponse(MIME_TEXT_PLAIN, stringToBytes(res.toString()));
    }
}
