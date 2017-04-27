package com.hazelcast.test.starter;

import com.google.common.io.Files;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

import java.io.File;
import java.io.IOException;

public class Downloader {
    private static final String MEMBER_URL = "https://repo1.maven.org/maven2/com/hazelcast/hazelcast/%1$s/hazelcast-%1$s.jar";
    private static final String CLIENT_URL = "https://repo1.maven.org/maven2/com/hazelcast/hazelcast-client/%1$s/hazelcast-client-%1$s.jar";

    public static File[] downloadVersion(String version, File target) {
        File[] files = new File[2];
        files[0] = downloadMember(version, target);
        files[1] = downloadClient(version, target);
        return files;
    }

    private static File downloadClient(String version, File target) {
        String url = constructUrlForClient(version);
        String filename = extractFilenameFromUrl(url);
        return downloadFile(url, target, filename);
    }

    private static File downloadMember(String version, File target) {
        String url = constructUrlForMember(version);
        String filename = extractFilenameFromUrl(url);
        return downloadFile(url, target, filename);
    }

    private static String extractFilenameFromUrl(String url) {
        int lastIndexOf = url.lastIndexOf('/');
        return url.substring(lastIndexOf);
    }

    private static File downloadFile(String url, File targetDirectory, String filename) {
        OkHttpClient client = new OkHttpClient();
        File targetFile = new File(targetDirectory, filename);
        if (targetFile.isFile() && targetFile.exists()) {
            return targetFile;
        }
        Request request = new Request.Builder()
                .url(url)
                .build();
        try {
            Response response = client.newCall(request).execute();
            if (!response.isSuccessful()) {
                throw new GuardianException("Cannot download file from " + url + ", http response code: " + response.code());
            }
            byte[] bytes = response.body().bytes();
            Files.write(bytes, targetFile);
            return targetFile;
        } catch (IOException e) {
            throw Utils.rethrow(e);
        }
    }

    private static String constructUrlForClient(String version) {
        return String.format(CLIENT_URL, version);
    }

    private static String constructUrlForMember(String version) {
        return String.format(MEMBER_URL, version);
    }
}
