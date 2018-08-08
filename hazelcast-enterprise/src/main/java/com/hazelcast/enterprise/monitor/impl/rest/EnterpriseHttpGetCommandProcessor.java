package com.hazelcast.enterprise.monitor.impl.rest;

import com.hazelcast.enterprise.monitor.LicenseInfo;
import com.hazelcast.instance.Node;
import com.hazelcast.internal.ascii.TextCommandService;
import com.hazelcast.internal.ascii.rest.HttpCommand;
import com.hazelcast.internal.ascii.rest.HttpGetCommand;
import com.hazelcast.internal.ascii.rest.HttpGetCommandProcessor;
import com.hazelcast.license.util.LicenseHelper;

import static com.hazelcast.internal.ascii.TextCommandConstants.MIME_TEXT_PLAIN;
import static com.hazelcast.util.StringUtil.stringToBytes;

class EnterpriseHttpGetCommandProcessor
        extends HttpGetCommandProcessor {

    private static final String URI_LICENSE_URL = "/hazelcast/rest/license";

    EnterpriseHttpGetCommandProcessor(TextCommandService textCommandService) {
        super(textCommandService);
    }

    @Override
    public void handle(HttpGetCommand command) {
        try {
            String uri = command.getURI();
            if (uri.startsWith(URI_LICENSE_URL)) {
                handleLicense(command);
                return;
            }

            super.handle(command);
        } catch (Exception e) {
            command.setResponse(HttpCommand.RES_500);
        } finally {
            textCommandService.sendResponse(command);
        }

    }

    private void handleLicense(HttpGetCommand command) {
        Node node = textCommandService.getNode();
        LicenseInfo licenseInfo = new LicenseInfoImpl(LicenseHelper.getLicense(
                node.getConfig().getLicenseKey(), node.getBuildInfo().getVersion()));

        StringBuilder res = new StringBuilder();
        res.append("licenseInfo").append(licenseInfo.toJson()).append("\n");
        command.setResponse(MIME_TEXT_PLAIN, stringToBytes(res.toString()));
    }
}
