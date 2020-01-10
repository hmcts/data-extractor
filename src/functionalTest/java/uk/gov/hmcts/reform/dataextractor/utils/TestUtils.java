package uk.gov.hmcts.reform.dataextractor.utils;

import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.microsoft.azure.storage.blob.ListBlobItem;
import org.testcontainers.shaded.org.apache.commons.io.FileUtils;
import uk.gov.hmcts.reform.dataextractor.DbTest;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.stream.StreamSupport;

import static org.junit.jupiter.api.Assertions.assertNotNull;


public final class TestUtils {

    /**
     * Returns string by reading from file given file path.
     *
     * @param filePath needed
     * @return string
     */
    public static String getDataFromFile(String filePath) {
        File file = file(filePath);
        try {
            return FileUtils.readFileToString(file, "UTF-8");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static InputStream getStreamFromFile(String filePath) {
        File file = file(filePath);
        try {
            return new FileInputStream(file);
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    private static File file(String filePath) {
        ClassLoader classLoader = DbTest.class.getClassLoader();
        return new File(classLoader.getResource(filePath).getFile());
    }

    public static boolean hasBlobThatStartsWith(CloudBlobContainer container, String filePrefix) {
        return StreamSupport.stream(container.listBlobs().spliterator(), false)
                .anyMatch(listBlobItem -> listBlobItem.getUri().getPath().startsWith(filePrefix));
    }

    public static CloudBlockBlob downloadFirstBlobThatStartsWith(CloudBlobContainer container, String filePrefix) {
        ListBlobItem firstBlob = container.listBlobs(filePrefix).iterator().next();
        assertNotNull(firstBlob.getStorageUri());
        try {
            return new CloudBlockBlob(firstBlob.getStorageUri());
        } catch (StorageException e) {
            throw new RuntimeException(e);
        }
    }

    private TestUtils() {
    }

}
