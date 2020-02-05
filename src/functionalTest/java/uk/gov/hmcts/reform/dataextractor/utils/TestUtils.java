package uk.gov.hmcts.reform.dataextractor.utils;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobListDetails;
import com.azure.storage.blob.models.ListBlobsOptions;
import org.testcontainers.shaded.org.apache.commons.io.FileUtils;

import uk.gov.hmcts.reform.dataextractor.DbTest;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;
import java.util.stream.StreamSupport;

import static org.junit.jupiter.api.Assertions.assertFalse;


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

    public static boolean hasBlobThatStartsWith(BlobContainerClient container, String filePrefix) {
        return StreamSupport.stream(container.listBlobs().spliterator(), false)
                .anyMatch(listBlobItem -> listBlobItem.getName().startsWith(filePrefix));
    }

    public static BlobClient downloadFirstBlobThatStartsWith(BlobContainerClient container, String filePrefix) {
        ListBlobsOptions options = new ListBlobsOptions();
        options.setPrefix(filePrefix);

        options.setDetails(new BlobListDetails().setRetrieveMetadata(true));
        Optional<BlobItem> blobItem = container.listBlobs(options, null)
                .stream()
                .findFirst();
        assertFalse(blobItem.isEmpty(), "BlobItem expected with prefix " + filePrefix);
        return container.getBlobClient(blobItem.get().getName());
    }

    private TestUtils() {
    }

}
