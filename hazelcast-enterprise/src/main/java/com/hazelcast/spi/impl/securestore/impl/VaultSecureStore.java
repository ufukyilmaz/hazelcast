package com.hazelcast.spi.impl.securestore.impl;

import com.hazelcast.config.SSLConfig;
import com.hazelcast.config.VaultSecureStoreConfig;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.json.Json;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.internal.json.JsonValue;
import com.hazelcast.internal.nio.ClassLoaderUtil;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.internal.util.StringUtil;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.ssl.BasicSSLContextFactory;
import com.hazelcast.nio.ssl.SSLContextFactory;
import com.hazelcast.spi.impl.securestore.SecureStoreException;

import javax.annotation.Nonnull;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * HashiCorp Vault Secure Store implementation.
 */
public class VaultSecureStore extends AbstractSecureStore {

    private final VaultClient client;

    VaultSecureStore(@Nonnull VaultSecureStoreConfig config, @Nonnull Node node) {
        super(config.getPollingInterval(), node);
        this.client = new VaultClient(config, node.getConfigClassLoader(), logger);
    }

    @Nonnull
    @Override
    public List<byte[]> retrieveEncryptionKeys() {
        return client.retrieveEncryptionKeys();
    }

    @Override
    protected Runnable getWatcher() {
        return new VaultWatcher();
    }

    /**
     * Detects changes in Vault by simply retrieving the current keys and comparing
     * them to the last keys. It would be nicer to use some sort of (lighter) notification
     * mechanism, but Vault does not seem to provide one. See for instance
     * https://github.com/hashicorp/vault/issues/1558
     */
    private final class VaultWatcher implements Runnable {
        private byte[] lastKey;

        private VaultWatcher() {
            this.lastKey = client.retrieveCurrentEncryptionKey();
        }

        @Override
        public void run() {
            try {
                byte[] currentKey = client.retrieveCurrentEncryptionKey();
                if (currentKey != null && !Arrays.equals(currentKey, lastKey)) {
                    logger.info("Vault encryption key change detected");
                    lastKey = currentKey;
                    notifyEncryptionKeyListeners(currentKey);
                }
            } catch (Exception e) {
                logger.warning("Error while detecting changes in Vault", e);
            }
        }
    }

    private static final class VaultClient {
        private static final int CONNECTION_TIMEOUT_MILLIS = 5000;
        private static final String HEADER_TOKEN = "X-Vault-Token";
        private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];
        private final VaultSecureStoreConfig config;
        // may be null
        private final ClassLoader classLoader;
        private final ILogger logger;

        private VaultClient(@Nonnull VaultSecureStoreConfig config, ClassLoader classLoader, @Nonnull ILogger logger) {
            this.config = config;
            this.classLoader = classLoader;
            this.logger = logger;
        }

        @Nonnull
        List<byte[]> retrieveEncryptionKeys() {
            try {
                String mountPath = getMountPath();
                // Vault does not support retrieving all versions of a secret at once,
                // so we first retrieve the versions metadata and then retrieve the
                // keys one by one
                Range range = readVersionRange(mountPath);
                List<byte[]> keys = new ArrayList<>(range.max - range.min + 1);
                for (int version = range.max; version >= range.min; version--) {
                    byte[] key = readKey(mountPath, version);
                    if (key != null) {
                        keys.add(key);
                    }
                }
                return keys;
            } catch (Exception e) {
                logger.warning("Failed to retrieve encryption keys", e);
                throw new SecureStoreException("Failed to retrieve encryption keys", e);
            }
        }

        private byte[] retrieveCurrentEncryptionKey() {
            try {
                String mountPath = getMountPath();
                return readKey(mountPath, 0);
            } catch (Exception e) {
                logger.warning("Failed to retrieve encryption keys", e);
                throw new SecureStoreException("Failed to retrieve the current encryption key", e);
            }
        }

        /**
         * Retrieves the specified version of the encryption key. Returns {@code null} if the secret
         * path does not exist.
         * <p>
         * Uses the {@code /v1/[mountPath]/data/[relativeSecretPath]} endpoint
         * (https://www.vaultproject.io/api/secret/kv/kv-v2.html#read-secret-version) in the case
         * of KV V2, and the {@code /v1/[secretPath]} endpoint
         * (https://www.vaultproject.io/api/secret/kv/kv-v1.html#read-secret) in the case of KV V1.
         * The version number is passed as the {@code version} request parameter (ignored by KV V1).
         * <p>
         * Sample KV V2 request URL for secret path {@code hz/cluster} (assuming that
         * {@code mountPath} is {@code hz/} - see {@link #getMountPath()}):
         * {@code http://127.0.0.1:8200/v1/hz/data/cluster?version=2}
         * <p>
         * Sample KV V2 response (only the relevant bits):
         * <pre>
         * {...
         *  "data":{
         *      "data":{"key name":"base64-encoded data"},
         *      "metadata":{...,
         *                  "version":2}
         *  },
         * ...}
         * </pre>
         * <p>
         * Sample KV V1 request URL for secret path {@code hz/cluster}:
         * {@code http://127.0.0.1:8200/v1/hz/cluster?version=2}
         * <p>
         * Sample KV V1 response (only the relevant bits):
         * <pre>
         * {...
         *  "data":{"key name":"base64-encoded data"},
         * ...}
         * </pre>
         *
         * @param mountPath the mount path or {@code null} (not a KV V2 secrets engine)
         * @param version   the version to retrieve (note that Vault treats 0 as the latest version)
         * @return the retrieved key bytes or {@code null} if the secret path does not exist
         */
        @SuppressWarnings("checkstyle:magicnumber")
        private byte[] readKey(String mountPath, int version) throws Exception {
            String requestUrl = String
                    .format("%s/v1/%s?version=%d", config.getAddress(), secretPath(config.getSecretPath(), mountPath, false),
                            version);
            Response response = doGet(requestUrl);
            if (response.statusCode == 404) {
                // key not found, ignore
                return null;
            }
            if (response.statusCode != 200) {
                throw new SecureStoreException("Unexpected response: " + response);
            }
            return parseKeyResponse(response, mountPath != null);
        }

        /**
         * Reads the available version range ([oldest, latest]) of the
         * encryption key. Returns [0, 0] for non-versioned secrets engines (no
         * request performed) or if the secret path was not found.
         * <p>
         * Uses the {@code /v1/[mountPath]/metadata/[relativeSecretPath]} endpoint
         * (https://www.vaultproject.io/api/secret/kv/kv-v2.html#list-secrets).
         * <p>
         * Sample request URL for secret path {@code hz/cluster} (assuming
         * that {@code mountPath} is {@code hz/} - see {@link #getMountPath()}):
         * {@code http://127.0.0.1:8200/v1/hz/metadata/cluster}
         * <p>
         * Sample response (only the relevant bits):
         * <pre>
         * {...
         *  "data":{
         *      ...
         *      "current_version":2,
         *      "oldest_version":0,
         *      "versions":{...},
         *  ...}
         * </pre>
         *
         * @param mountPath the mount path or {@code null} (not a KV V2 secrets engine)
         * @return a version range ([0,0] if the the secrets engine is not KV V2 or if
         * the secrets path does not exist)
         */
        @SuppressWarnings("checkstyle:magicnumber")
        private Range readVersionRange(String mountPath) throws Exception {
            if (mountPath == null) {
                // only KV V2 supports versioning
                return new Range(0, 0);
            }
            String secretPath = secretPath(config.getSecretPath(), mountPath, true);
            String requestUrl = String.format("%s/v1/%s", config.getAddress(), secretPath);
            Response response = doGet(requestUrl);
            if (response.statusCode == 404) {
                logger.warning("No key metadata found at secret path: " + secretPath);
                return new Range(0, 0);
            }
            if (response.statusCode != 200) {
                throw new SecureStoreException("Unexpected response: " + response);
            }
            return parseVersionResponse(response);
        }

        private static Range parseVersionResponse(Response response) {
            String jsonString = StringUtil.bytesToString(response.body);
            JsonObject jsonObject = Json.parse(jsonString).asObject();
            jsonObject = jsonObject.get("data").asObject();
            int min = 1;
            int max = 0;
            for (JsonObject.Member member : jsonObject) {
                String memberName = member.getName();
                if ("oldest_version".equals(memberName)) {
                    // Vault may emit 0 as oldest_version
                    min = Math.max(intValue(member), 1);
                } else if ("current_version".equals(memberName)) {
                    max = intValue(member);
                }
            }
            if (max == 0) {
                throw new SecureStoreException("Failed to parse version range: " + response);
            }
            return new Range(min, max);
        }

        private static int intValue(JsonObject.Member member) {
            JsonValue jsonValue = member.getValue();
            if (jsonValue != null && jsonValue.isNumber()) {
                return jsonValue.asInt();
            }
            return 0;
        }

        private static byte[] parseKeyResponse(Response response, boolean kvV2) {
            String jsonString = StringUtil.bytesToString(response.body);
            JsonObject jsonObject = Json.parse(jsonString).asObject();
            jsonObject = jsonObject.get("data").asObject();
            if (kvV2) {
                jsonObject = jsonObject.get("data").asObject();
            }
            String value = null;
            for (JsonObject.Member member : jsonObject) {
                if (value != null) {
                    throw new SecureStoreException("Multiple key/value mappings found under secret path");
                }
                JsonValue jsonValue = member.getValue();
                if (jsonValue != null && !jsonValue.isNull()) {
                    value = jsonValue.isString() ? jsonValue.asString() : jsonValue.toString();
                }
            }
            return value == null ? null : decodeKey(value);
        }

        private static byte[] decodeKey(String base64Value) {
            try {
                return Base64.getDecoder().decode(base64Value.trim());
            } catch (Exception e) {
                throw new SecureStoreException("Failed to Base64-decode encryption key", e);
            }
        }

        /**
         * Returns the mount path for KV V2 secrets engines, {@code null} otherwise.
         * <p>
         * Uses the {@code /sys/internal/ui/mounts} endpoint (https://www.vaultproject.io/api/system/internal-ui-mounts.html).
         * <p>
         * Sample request URL for secret path {@code hz/cluster}:
         * {@code http://127.0.0.1:8200/v1/sys/internal/ui/mounts/hz/cluster}
         * <p>
         * Sample response (only the relevant bits):
         * <pre>
         * {...
         *  "data":{
         *      ...
         *      "options":{
         *              "version":"2",
         *              ...
         *            },
         *      "path":"hz/",
         *      "type":"kv",
         *      ...
         *  },
         *  ...
         * }
         * </pre>
         *
         * @return the mount path or {@code null} if the secret path does not belong to a KV V2 secrets engine
         */
        @SuppressWarnings("checkstyle:magicnumber")
        private String getMountPath() throws Exception {
            String requestUrl = String.format("%s/v1/sys/internal/ui/mounts/%s", config.getAddress(), config.getSecretPath());
            Response response = doGet(requestUrl);
            if (response.statusCode == 404) {
                logger.fine("The Vault /sys/internal/ui/mounts endpoint not found, assuming V1");
                return null;
            }
            if (response.statusCode != 200) {
                throw new SecureStoreException(
                        "Failed to determine the secrets engine mount path for secret path: " + config.getSecretPath() + ": "
                                + response);
            }
            return parseMountsResponse(response);
        }

        private static String parseMountsResponse(Response response) {
            String jsonString = StringUtil.bytesToString(response.body);
            JsonObject jsonObject = Json.parse(jsonString).asObject();
            jsonObject = jsonObject.get("data").asObject();
            String type = jsonObject.getString("type", null);
            if (!"kv".equals(type)) {
                throw new SecureStoreException("Unsupported secrets engine type: " + type);
            }
            JsonObject options = jsonObject.get("options").asObject();
            String version = options.getString("version", "0");
            // a secrets engine version is a string
            if (!"2".equals(version)) {
                return null;
            }
            return jsonObject.getString("path", null);
        }

        private static String secretPath(String path, String mountPath, boolean metadata) {
            if (mountPath == null) {
                // not a KV V2: use the path as is
                return path;
            }
            /* V2 requires that the read path contains "data"/"metadata" between the
               secrets engine path and the secret path itself. */
            StringBuilder joined = new StringBuilder(mountPath);
            joined.append(metadata ? "metadata/" : "data/");
            int index = path.indexOf(mountPath);
            assert index != -1;
            String subPath = path.substring(index + mountPath.length());
            joined.append(subPath);
            return joined.toString();
        }

        private Response doGet(String requestUrl) throws Exception {
            Map<String, String> headers = new HashMap<>();
            headers.put(HEADER_TOKEN, config.getToken());
            HttpURLConnection connection = null;
            try {
                connection = getConnection(requestUrl, config.getSSLConfig());
                connection.setRequestMethod("GET");
                connection.setRequestProperty("Connection", "close");
                connection.setConnectTimeout(CONNECTION_TIMEOUT_MILLIS);
                connection.setReadTimeout(CONNECTION_TIMEOUT_MILLIS);
                for (Map.Entry<String, String> header : headers.entrySet()) {
                    connection.setRequestProperty(header.getKey(), header.getValue());
                }
                int statusCode = connection.getResponseCode();
                String contentType = connection.getContentType();
                byte[] body = responseBody(connection);
                return new Response(statusCode, contentType, body);
            } finally {
                if (connection != null) {
                    connection.disconnect();
                }
            }
        }

        private HttpURLConnection getConnection(String urlString, SSLConfig sslConfig) throws Exception {
            URL url = new URL(urlString);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            if (connection instanceof HttpsURLConnection) {
                if (sslConfig == null || !sslConfig.isEnabled()) {
                    throw new SecureStoreException("SSL/TLS not enabled in the configuration");
                }
                SSLContext sslContext = loadSSLContextFactory(sslConfig).getSSLContext();
                ((HttpsURLConnection) connection).setSSLSocketFactory(sslContext.getSocketFactory());
            }
            return connection;
        }

        private SSLContextFactory loadSSLContextFactory(SSLConfig config) throws Exception {
            Object implementation = config.getFactoryImplementation();
            String factoryClassName = config.getFactoryClassName();
            if (implementation == null && factoryClassName != null) {
                implementation = ClassLoaderUtil.newInstance(classLoader, factoryClassName);
            }
            if (implementation == null) {
                implementation = new BasicSSLContextFactory();
            }
            SSLContextFactory sslContextFactory = (SSLContextFactory) implementation;
            sslContextFactory.init(config.getProperties());
            return sslContextFactory;
        }

        private static byte[] responseBody(HttpURLConnection conn) throws IOException {
            InputStream in;
            int responseCode = conn.getResponseCode();
            if (responseCode < HttpURLConnection.HTTP_BAD_REQUEST) {
                in = conn.getInputStream();
            } else {
                in = conn.getErrorStream();
            }
            if (in != null) {
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                IOUtil.drainTo(in, out);
                return out.toByteArray();
            }
            return EMPTY_BYTE_ARRAY;
        }

        private static final class Response {
            private final int statusCode;
            private final String contentType;
            private final byte[] body;

            private Response(int statusCode, String contentType, byte[] body) {
                this.statusCode = statusCode;
                this.contentType = contentType;
                this.body = body;
            }

            @Override
            public String toString() {
                return "Response{"
                        + "statusCode: " + statusCode
                        + ", contentType: " + contentType
                        + ", body: " + StringUtil.bytesToString(body)
                        + '}';
            }
        }

        private static final class Range {
            private final int min;
            private final int max;

            private Range(int min, int max) {
                this.min = min;
                this.max = max;
            }
        }
    }
}
