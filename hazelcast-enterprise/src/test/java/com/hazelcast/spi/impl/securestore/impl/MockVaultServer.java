package com.hazelcast.spi.impl.securestore.impl;

import com.google.common.collect.ImmutableMap;
import com.hazelcast.config.SSLConfig;
import com.hazelcast.config.VaultSecureStoreConfig;
import com.hazelcast.internal.nio.ClassLoaderUtil;
import com.hazelcast.internal.util.StringUtil;
import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsParameters;
import com.sun.net.httpserver.HttpsServer;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.TrustManagerFactory;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.security.KeyStore;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A simple Vault server mock for testing the functionality used by the Vault Secure Store.
 */
public class MockVaultServer {
    private static final InetAddress LOOPBACK_ADDRESS = InetAddress.getLoopbackAddress();

    private enum SecretsEngine {
        KV_V1("a/", "kv", "1"),
        KV_V2("b/", "kv", "2"),
        NONKV("c/", "foo", "1");

        private final String mount;
        private final String type;
        // In Vault, version is a string
        private final String version;

        SecretsEngine(String mount, String type, String version) {
            this.mount = mount;
            this.type = type;
            this.version = version;
        }
    }

    public static final String TOKEN = "gRAz1nG.5HeEp";
    public static final String SECRET_PATH = SecretsEngine.KV_V2.mount + "acme/cluster1";
    public static final String SECRET_PATH_WITHOUT_SECRET = SecretsEngine.KV_V2.mount + "acme/cluster1/nosecret";
    public static final String SECRET_PATH_MULTIPLE_SECRETS = SecretsEngine.KV_V2.mount + "acme/cluster1/multisecret";
    public static final String SECRET_PATH_INVALID_BASE64 = SecretsEngine.KV_V2.mount + "acme/cluster1/invalid-b64";
    public static final String SECRET_PATH_UNUSABLE_KEY = SecretsEngine.KV_V2.mount + "acme/cluster1/unusable";
    public static final String SECRET_PATH_V1 = SecretsEngine.KV_V1.mount + "acme/cluster1";
    public static final String SECRET_PATH_NONKV = SecretsEngine.NONKV.mount + "acme/cluster1";
    public static final byte[] KEY1 = new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6};
    private static final String KEY1_BASE64 = Base64.getEncoder().encodeToString(KEY1);
    public static final byte[] KEY2 = new byte[]{6, 5, 4, 3, 2, 1, 0, 9, 8, 7, 6, 5, 4, 3, 2, 1};
    private static final String KEY2_BASE64 = Base64.getEncoder().encodeToString(KEY2);
    private static final byte[] UNUSABLE_KEY = new byte[]{0};
    private static final String UNUSABLE_KEY_BASE64 = Base64.getEncoder().encodeToString(UNUSABLE_KEY);

    private static final String RESP_PERMISSION_DENIED = "{\"errors\":[\"permission denied\"]}";
    private static final String RESP_EMPTY = "{\"errors\":[]}";

    private static final char[] TEST_KEYSTORE_PASSWORD = "password".toCharArray();

    private static final String MOUNTS_ENDPOINT = "sys/internal/ui/mounts/";

    private final InetSocketAddress address;
    private final File keyStoreFile; // for https
    private HttpServer server;
    // secret path -> history of key/value mappings (last one is the most recent one)
    private Map<String, List<Map<String, String>>> mappingsPerSecretPath;

    private MockVaultServer(InetSocketAddress address, File keyStoreFile) {
        this.address = address;
        this.keyStoreFile = keyStoreFile;
        this.mappingsPerSecretPath = defaultMappings();
    }

    private static Map<String, List<Map<String, String>>> defaultMappings() {
        Map<String, List<Map<String, String>>> keys = new ConcurrentHashMap<>();
        keys.put(SECRET_PATH, versions(simpleMapping(KEY1_BASE64), simpleMapping(KEY2_BASE64)));
        keys.put(SECRET_PATH_V1, versions(simpleMapping(KEY1_BASE64), simpleMapping(KEY2_BASE64)));
        keys.put(SECRET_PATH_WITHOUT_SECRET, versions());
        keys.put(SECRET_PATH_MULTIPLE_SECRETS, versions(ImmutableMap.of("one", KEY1_BASE64, "two", KEY2_BASE64)));
        keys.put(SECRET_PATH_INVALID_BASE64, versions(simpleMapping("not-base64")));
        keys.put(SECRET_PATH_UNUSABLE_KEY, versions(simpleMapping(UNUSABLE_KEY_BASE64)));
        return keys;
    }

    private static List<Map<String, String>> versions(Map<String, String>... versions) {
        return Stream.of(versions).collect(Collectors.collectingAndThen(Collectors.toList(), CopyOnWriteArrayList::new));
    }

    private static Map<String, String> simpleMapping(String value) {
        return Collections.singletonMap("value", value);
    }

    /**
     * Adds a new secret version under the specified secret path.
     */
    public void addMappingVersion(String secretPath, String key) {
        mappingsPerSecretPath.computeIfAbsent(secretPath, k -> new CopyOnWriteArrayList<>()).add(simpleMapping(key));
    }

    /**
     * Removes all mappings under the specified secret path
     */
    public void clearMappings(String secretPath) {
        mappingsPerSecretPath.remove(secretPath);
    }

    /**
     * Reverts all secrets mappings to the default/initial state.
     */
    public void revertMappings() {
        mappingsPerSecretPath = defaultMappings();
    }

    private final class VaultHandler implements HttpHandler {

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            Headers headers = exchange.getRequestHeaders();
            String requestToken = headers.getFirst("X-Vault-Token");
            if (!TOKEN.equals(requestToken)) {
                writeResponse(exchange, 403, RESP_PERMISSION_DENIED);
                return;
            }

            URI requestURI = exchange.getRequestURI();
            String path = requestURI.toString();

            if (path.contains(MOUNTS_ENDPOINT)) {
                path = path.replace(MOUNTS_ENDPOINT, "");
                path = path.replaceFirst("/v1/", "");
                handleMounts(exchange, path);
                return;
            }

            String namespace = headers.getFirst("X-Vault-Namespace");
            if (namespace != null) {
                path = path.replaceFirst("/v1/", namespace + '/');
            } else {
                path = path.replaceFirst("/v1/", "");
            }

            if (path.contains("/metadata/")) {
                handleMetadata(exchange, path);
            } else {
                handleData(exchange, path);
            }
        }

        private void handleMounts(HttpExchange exchange, String path) throws IOException {
            for (SecretsEngine engine : SecretsEngine.values()) {
                if (path.startsWith(engine.mount)) {
                    StringBuilder success = new StringBuilder("{\"request_id\":\"c66246ee-fee5-fccc-be54-122c6771f5d2\"");
                    success.append(",\"lease_id\":\"\"");
                    success.append(",\"renewable\":false");
                    success.append(",\"lease_duration\":0");
                    success.append(",\"data\":{\"accessor\":\"kv_eecc8cd9\"");
                    success.append(",\"config\":{\"default_lease_ttl\":0,\"force_no_cache\":false,\"max_lease_ttl\":0}");
                    success.append(",\"description\":\"\"");
                    success.append(",\"local\":false");
                    success.append(",\"options\":{\"version\":\"").append(engine.version).append("\"}");
                    success.append(",\"path\":\"").append(engine.mount).append("\"");
                    success.append(",\"seal_wrap\":false");
                    success.append(",\"type\":\"").append(engine.type).append("\"");
                    success.append(",\"uuid\":\"51d9b4bd-b9d5-590d-7948-59a435545253\"}");
                    success.append(",\"wrap_info\":null");
                    success.append(",\"warnings\":null");
                    success.append(",\"auth\":null}");
                    writeResponse(exchange, 200, success.toString());
                    return;
                }
            }
            // Vault responds with 403 if mount cannot be found
            writeResponse(exchange, 403, RESP_PERMISSION_DENIED);
        }

        private String getPlainPath(String path) {
            for (SecretsEngine engine : SecretsEngine.values()) {
                if ("kv".equals(engine.type) && "2".equals(engine.version)) {
                    // remove the "data"/"metadata" step after the mount path
                    path = path.replace(engine.mount + "data/", engine.mount);
                    path = path.replace(engine.mount + "metadata/", engine.mount);
                    return path;
                }
            }
            return path;
        }

        private void handleMetadata(HttpExchange exchange, String path) throws IOException {
            String dataPath = getPlainPath(path);
            List<Map<String, String>> mappings = mappingsPerSecretPath.get(dataPath);
            if (mappings == null || mappings.isEmpty()) {
                writeResponse(exchange, 404, RESP_EMPTY);
                return;
            }
            StringBuilder success = new StringBuilder("{\"request_id\":\"b3161891-f830-3ca5-d358-96e0fa6679bf\"");
            success.append(",\"lease_id\":\"\"");
            success.append(",\"renewable\":false");
            success.append(",\"lease_duration\":0");
            success.append(",\"data\":{");
            success.append("\"cas_required\":false");
            success.append(",\"created_time\":\"2019-07-29T07:15:42.858185Z\"");
            success.append(",\"current_version\":").append(mappings.size());
            success.append(",\"max_versions\":0");
            success.append(",\"oldest_version\":1");
            success.append(",\"updated_time\":\"2019-07-29T10:41:02.585197Z\"");
            success.append(",\"versions\":{");
            for (int i = 1; i <= mappings.size(); i++) {
                if (i > 1) {
                    success.append(',');
                }
                success.append("\"").append(i).append("\":{");
                success.append("\"created_time\":\"2019-07-29T07:15:42.858185Z\",\"deletion_time\":\"\",\"destroyed\":false");
                success.append('}');
            }
            success.append('}');
            success.append('}');
            success.append('}');
            writeResponse(exchange, 200, success.toString());
        }

        private void handleData(HttpExchange exchange, String path) throws IOException {
            String pathRaw;
            int params = path.indexOf('?');
            if (params != -1) {
                pathRaw = path.substring(0, params);
            } else {
                pathRaw = path;
            }
            String pathPlain = getPlainPath(pathRaw);
            boolean kvv2 = !pathPlain.equals(pathRaw);
            List<Map<String, String>> mappings = mappingsPerSecretPath.get(pathPlain);
            if (mappings == null) {
                writeResponse(exchange, 404, RESP_EMPTY);
                return;
            }

            // here we assume that (if URL parameters are present)
            // there is only one "version=int" parameter
            int version;
            if (params != -1) {
                int eqIndex = path.lastIndexOf('=');
                String versionStr = path.substring(eqIndex + 1);
                version = Integer.parseInt(versionStr);
                if (version == 0) {
                    // in Vault, 0 corresponds to the current version
                    version = mappings.size();
                }
            } else {
                version = mappings.size();
            }

            int versionIndex = version - 1;
            Map<String, String> mappingVersion = null;
            for (Map<String, String> mapping : mappings) {
                if (versionIndex == 0) {
                    mappingVersion = mapping;
                    break;
                } else {
                    versionIndex--;
                }
            }
            if (mappingVersion == null) {
                writeResponse(exchange, 404, RESP_EMPTY);
            } else {
                StringBuilder success = new StringBuilder();
                if (kvv2) {
                    success.append("{\"request_id\":\"b3161891-f830-3ca5-d358-96e0fa6679bf\"");
                    success.append(",\"lease_id\":\"\"");
                    success.append(",\"renewable\":false");
                    success.append(",\"lease_duration\":0");
                    success.append(",\"data\":{");
                    success.append("\"data\":{");
                    appendMapping(mappingVersion, success);
                    success.append('}');
                    success.append(",\"metadata\":{");
                    success.append("\"created_time\":\"2019-06-21T12:30:27.324583Z\",\"deletion_time\":\"\"");
                    success.append(",\"destroyed\":false,\"version\":").append(version);
                    success.append('}');
                    success.append('}');
                    success.append(",\"wrap_info\":null");
                    success.append(",\"warnings\":null");
                    success.append(",\"auth\":null");
                    success.append('}');
                } else {
                    success.append("{\"request_id\":\"3785a4b5-8208-16d3-ff5e-35c4db97b0fc\"");
                    success.append(",\"lease_id\":\"\"");
                    success.append(",\"renewable\":false");
                    success.append(",\"lease_duration\":2764800");
                    success.append(",\"data\":{");
                    appendMapping(mappingVersion, success);
                    success.append('}');
                    success.append(",\"wrap_info\":null");
                    success.append(",\"warnings\":null");
                    success.append(",\"auth\":null");
                    success.append('}');
                }
                writeResponse(exchange, 200, success.toString());
            }
        }

        private void appendMapping(Map<String, String> mapping, StringBuilder jsonOut) {
            boolean first = true;
            for (Map.Entry<String, String> e : mapping.entrySet()) {
                if (first) {
                    first = false;
                } else {
                    jsonOut.append(',');
                }
                jsonOut.append("\"").append(e.getKey()).append("\":\"").append(e.getValue()).append("\"");
            }

        }

        private void writeResponse(HttpExchange t, int responseCode, String responseBody) throws IOException {
            byte[] responseBytes = StringUtil.stringToBytes(responseBody);
            Headers responseHeaders = t.getResponseHeaders();
            responseHeaders.add("Content-Type", "application/json");
            responseHeaders.add("Content-Length", String.valueOf(responseBytes.length));
            responseHeaders.add("Connection", "close");
            t.sendResponseHeaders(responseCode, responseBytes.length);
            OutputStream os = t.getResponseBody();
            os.write(responseBytes);
            os.close();
        }
    }

    private void start() throws Exception {
        server = keyStoreFile == null ? HttpServer.create(address, 0) : HttpsServer.create(address, 0);
        if (keyStoreFile != null) {
            SSLContext sslContext = SSLContext.getInstance("TLS");

            KeyManagerFactory kmf = KeyManagerFactory.getInstance(keyManagerAlgorithmName());
            KeyStore ks = KeyStore.getInstance("PKCS12");
            try (FileInputStream in = new FileInputStream(keyStoreFile)) {
                ks.load(in, TEST_KEYSTORE_PASSWORD);
            }
            kmf.init(ks, TEST_KEYSTORE_PASSWORD);

            TrustManagerFactory tmf = TrustManagerFactory.getInstance(keyManagerAlgorithmName());
            tmf.init(ks);

            sslContext.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);
            ((HttpsServer) server).setHttpsConfigurator(new HttpsConfigurator(sslContext) {
                public void configure(HttpsParameters params) {
                    try {
                        SSLContext context = getSSLContext();
                        SSLEngine engine = context.createSSLEngine();
                        params.setNeedClientAuth(false);
                        params.setCipherSuites(engine.getEnabledCipherSuites());
                        params.setProtocols(engine.getEnabledProtocols());
                        SSLParameters sslParameters = context.getSupportedSSLParameters();
                        params.setSSLParameters(sslParameters);
                    } catch (Exception ex) {
                        throw new AssertionError("Failed to create HTTPS port");
                    }
                }
            });
        }

        server.createContext("/", new VaultHandler());
        server.setExecutor(null); // default single-threaded executor
        server.start();
    }

    public void stop() {
        if (server != null) {
            server.stop(0);
        }
    }

    public static MockVaultServer start(File httpsKeyStoreFile, VaultSecureStoreConfig vaultConfig) throws Exception {
        SSLConfig sslConfig = null;
        if (httpsKeyStoreFile != null) {
            createKeyStore(httpsKeyStoreFile);
            sslConfig = new SSLConfig();
            sslConfig.setProperty("keyStore", httpsKeyStoreFile.getAbsolutePath());
            sslConfig.setProperty("keyStorePassword", "password");
            sslConfig.setProperty("keyManagerAlgorithm", keyManagerAlgorithmName());
            sslConfig.setProperty("keyStoreType", "PKCS12");
            sslConfig.setProperty("trustStore", httpsKeyStoreFile.getAbsolutePath());
            sslConfig.setProperty("trustStorePassword", "password");
            sslConfig.setProperty("trustManagerAlgorithm", keyManagerAlgorithmName());
            sslConfig.setProperty("trustStoreType", "PKCS12");
            sslConfig.setEnabled(true);
        }
        InetSocketAddress address = new InetSocketAddress(LOOPBACK_ADDRESS, 4999);
        vaultConfig.setAddress(getUrl(address, httpsKeyStoreFile != null)).setSSLConfig(sslConfig);
        MockVaultServer vault = new MockVaultServer(address, httpsKeyStoreFile);
        vault.start();
        return vault;
    }

    private static String getUrl(InetSocketAddress address, boolean https) {
        return (https ? "https://" : "http://") + address.getHostName() + ":" + address.getPort();
    }

    private static void createKeyStore(File keyStoreFile) throws Exception {
        keytool(String.format("-genkey -keyalg RSA -alias selfsigned -keystore %s -storetype PKCS12 -storepass password"
                        + " -validity 3650 -keysize 2048 -dname CN=localhost -ext san=ip:%s", keyStoreFile.getAbsolutePath(),
                LOOPBACK_ADDRESS.getHostAddress()));
    }

    private static void keytool(String cmdline) throws Exception {
        Class<?> clazz = ClassLoaderUtil.loadClass(null, keyToolClassName());
        Method main = clazz.getMethod("main", String[].class);
        main.invoke(null, (Object) cmdline.trim().split("\\s+"));
    }

    private static boolean isIbm() {
        String vendor = System.getProperty("java.vendor");
        return vendor != null && vendor.toLowerCase().contains("ibm");
    }

    private static String keyManagerAlgorithmName() {
        return isIbm() ? "IbmX509" : "SunX509";
    }

    private static String keyToolClassName() {
        return isIbm() ? "com.ibm.crypto.tools.KeyTool" : "sun.security.tools.keytool.Main";
    }
}
