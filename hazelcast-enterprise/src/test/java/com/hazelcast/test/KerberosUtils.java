package com.hazelcast.test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Map;

import javax.security.auth.kerberos.KerberosPrincipal;

import org.apache.directory.server.kerberos.kdc.KdcServer;
import org.apache.directory.server.kerberos.shared.crypto.encryption.KerberosKeyFactory;
import org.apache.directory.server.kerberos.shared.keytab.Keytab;
import org.apache.directory.server.kerberos.shared.replay.ReplayCache;
import org.apache.directory.shared.kerberos.KerberosTime;
import org.apache.directory.shared.kerberos.codec.types.EncryptionType;
import org.apache.directory.shared.kerberos.components.EncryptionKey;

import com.hazelcast.config.security.RealmConfig;
import com.hazelcast.logging.Logger;
import com.hazelcast.security.impl.SecurityUtil;

public final class KerberosUtils {

    /**
     * Creates {@link RealmConfig} for JAAS Kerberos authentication (using
     * {@code com.sun.security.auth.module.Krb5LoginModule}).
     *
     * @param principal principal to be authenticated
     * @param keytabPath file path to a keytab containing principal's secret
     * @param isInitiator true means client side of the kerberos protocol is generated
     * @return {@link RealmConfig} instance
     */
    public static RealmConfig createKerberosJaasRealmConfig(String principal, String keytabPath, boolean isInitiator) {
        RealmConfig krbRealm = SecurityUtil.createKerberosJaasRealmConfig(principal, keytabPath, isInitiator);
        return krbRealm;
    }

    /**
     * Creates a keytab file for given principal.
     *
     * @param principalName
     * @param passPhrase
     * @param keytabFile
     * @throws IOException
     * @return Returns the keytabFile - as provided in the argument
     */
    public static File createKeytab(final String principalName, final String passPhrase, final File keytabFile)
            throws IOException {
        final KerberosTime timeStamp = new KerberosTime();

        try (DataOutputStream dos = new DataOutputStream(new FileOutputStream(keytabFile))) {
            dos.write(Keytab.VERSION_0X502_BYTES);

            for (Map.Entry<EncryptionType, EncryptionKey> keyEntry : KerberosKeyFactory
                    .getKerberosKeys(principalName, passPhrase).entrySet()) {
                final EncryptionKey key = keyEntry.getValue();
                final byte keyVersion = (byte) key.getKeyVersion();
                // entries.add(new KeytabEntry(principalName, principalType, timeStamp, keyVersion, key));

                final ByteArrayOutputStream baos = new ByteArrayOutputStream();
                // handle principal name
                String[] spnSplit = principalName.split("@");
                String nameComponent = spnSplit[0];
                String realm = spnSplit[1];

                String[] nameComponents = nameComponent.split("/");
                try (DataOutputStream entryDos = new DataOutputStream(baos)) {
                    // increment for v1
                    entryDos.writeShort((short) nameComponents.length);
                    entryDos.writeUTF(realm);
                    // write components
                    for (String component : nameComponents) {
                        entryDos.writeUTF(component);
                    }

                    entryDos.writeInt(1); // principal type: KRB5_NT_PRINCIPAL
                    entryDos.writeInt((int) (timeStamp.getTime() / 1000));
                    entryDos.write(keyVersion);

                    entryDos.writeShort((short) key.getKeyType().getValue());

                    byte[] data = key.getKeyValue();
                    entryDos.writeShort((short) data.length);
                    entryDos.write(data);
                }
                final byte[] entryBytes = baos.toByteArray();
                dos.writeInt(entryBytes.length);
                dos.write(entryBytes);
            }
        }
        return keytabFile;
    }

    /**
     * Workaround for https://issues.apache.org/jira/browse/DIRKRB-744. Injects a dummy replay cache to given KdcServer.
     */
    public static void injectDummyReplayCache(KdcServer kdcServer) {
        try {
            Field field = kdcServer.getClass().getDeclaredField("replayCache");
            field.setAccessible(true);
            field.set(kdcServer, new NocheckReplayCache());
        } catch (Exception e) {
            Logger.getLogger(KerberosUtils.class).warning("NocheckReplayCache was not injected to the KdcServer.", e);
        }
    }

    public static class NocheckReplayCache implements ReplayCache {

        @Override
        public boolean isReplay(KerberosPrincipal serverPrincipal, KerberosPrincipal clientPrincipal, KerberosTime clientTime,
                int clientMicroSeconds) {
            return false;
        }

        @Override
        public void save(KerberosPrincipal serverPrincipal, KerberosPrincipal clientPrincipal, KerberosTime clientTime,
                int clientMicroSeconds) {
        }

        @Override
        public void clear() {
        }
    }
}
