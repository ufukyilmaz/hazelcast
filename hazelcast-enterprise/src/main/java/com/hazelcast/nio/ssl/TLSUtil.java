package com.hazelcast.nio.ssl;

import java.security.cert.Certificate;
import java.util.Map;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLPeerUnverifiedException;

final class TLSUtil {

    private static final Certificate[] EMPTY_CERTS = new Certificate[0];

    private TLSUtil() {
    }

    /**
     * Puts peer certificates (if any) from given {@link SSLEngine} to the provided {@code attributeMap} under
     * the {@code java.security.Certificate.class} key.
     */
    static void publishRemoteCertificates(SSLEngine sslEngine, Map attributeMap) {
        if (sslEngine.getUseClientMode() || sslEngine.getNeedClientAuth() || sslEngine.getWantClientAuth()) {
            Certificate[] certs;
            try {
                certs = sslEngine.getSession().getPeerCertificates();
            } catch (SSLPeerUnverifiedException e) {
                certs = EMPTY_CERTS;
            }
            attributeMap.putIfAbsent(Certificate.class, certs);
        }
    }
}
