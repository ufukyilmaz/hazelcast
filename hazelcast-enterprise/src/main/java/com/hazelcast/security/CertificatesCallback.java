package com.hazelcast.security;

import java.security.cert.Certificate;

import javax.security.auth.callback.Callback;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Callback for retrieving TLS certificates from the underlying connection.
 */
@SuppressFBWarnings({ "EI_EXPOSE_REP", "EI_EXPOSE_REP2" })
public class CertificatesCallback implements Callback {

    private Certificate[] certificates;

    public void setCertificates(Certificate[] certificates) {
        this.certificates = certificates;
    }

    /**
     * @return an ordered array of TLS client certificates, with the client's own certificate first followed by any certificate
     *         authorities.
     */
    public Certificate[] getCertificates() {
        return certificates;
    }
}
