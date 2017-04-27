package info.jerrinot.compatibilityguardian;

import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import static org.junit.Assert.*;

public class DownloaderTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void downloadVersion() throws IOException {
        File[] files = Downloader.downloadVersion("3.6", folder.getRoot());
        HashFunction md5Hash = Hashing.md5();

        byte[] memberBytes = Files.readAllBytes(files[0].toPath());
        HashCode memberHash = md5Hash.hashBytes(memberBytes);
        assertEquals("89563f7dab02bd5f592082697c24d167", memberHash.toString());

        byte[] clientBytes = Files.readAllBytes(files[1].toPath());
        HashCode clientHash = md5Hash.hashBytes(clientBytes);
        assertEquals("fd6022e35908b42d24fe10a9c9fdaad5", clientHash.toString());
    }

}