package uk.gov.hmcts.reform.dataextractor;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.shaded.org.apache.commons.io.IOUtils;
import uk.gov.hmcts.reform.dataextractor.utils.TestUtils;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.NoSuchElementException;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
@ExtendWith(MockitoExtension.class)
public class BlobOutputWriterTest {

    private CloudBlobClient cloudBlobClient;

    private static final String CLIENT_ID = "testclientid";
    private static final String CONTAINER = "testcontainer";
    private static final String BLOB_PREFIX = "testblob";
    private static final String ACCOUNT = "devstoreaccount1";

    @Container
    public static final GenericContainer blobStorageContainer =
        new GenericContainer("arafato/azurite:2.6.5")
            .withEnv("executable", "blob")
            .withExposedPorts(10000);


    @BeforeEach
    public void setUp() throws URISyntaxException, InvalidKeyException, StorageException {
        blobStorageContainer.start();
        Integer blobMappedPort = blobStorageContainer.getMappedPort(10000);
        String connString = "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;"
            + "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;"
            + "BlobEndpoint=http://127.0.0.1:" + blobMappedPort + "/devstoreaccount1;";
        CloudStorageAccount account = CloudStorageAccount.parse(connString);
        this.cloudBlobClient = account.createCloudBlobClient();
        this.cloudBlobClient.getContainerReference(CONTAINER).create();
    }

    @AfterEach
    public void tearDown() {
        blobStorageContainer.stop();
    }

    @Test
    public void whenBlobOutputWriterCreated_thenBufferedOutputAvailable() {
        try (BlobOutputWriter writer = new BlobOutputWriter(
                CLIENT_ID, ACCOUNT, CONTAINER, BLOB_PREFIX, DataExtractorApplication.Output.JSON_LINES)) {
            OutputStream outputStream = writer.outputStream(cloudBlobClient);
            assertThat(outputStream, instanceOf(BufferedOutputStream.class));
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"dataA1.json", "dataA1.csv"})
    public void whenfileUploaded_thenAvailableInBlobStorage(String filePath) throws Exception {
        try (BlobOutputWriter writer = new BlobOutputWriter(
            CLIENT_ID, ACCOUNT, CONTAINER, BLOB_PREFIX, DataExtractorApplication.Output.JSON_LINES)) {
            OutputStream outputStream = writer.outputStream(cloudBlobClient);
            assertNotNull(outputStream);
            InputStream inputStream = TestUtils.getStreamFromFile(filePath);
            IOUtils.copy(inputStream, outputStream);
            outputStream.flush();
            outputStream.close();
        }
        // retrieve uploaded blob
        CloudBlobContainer container = cloudBlobClient.getContainerReference(CONTAINER);
        TestUtils.hasBlobThatStartsWith(container, BLOB_PREFIX);
        CloudBlockBlob blob = TestUtils.downloadFirstBlobThatStartsWith(container, BLOB_PREFIX);
        assertTrue(blob.exists());
        assertEquals(TestUtils.getDataFromFile(filePath), blob.downloadText());
    }

    @Test
    public void whenfileNotUploaded_thenMissingFromBlobStorage() throws Exception {
        CloudBlobContainer container = cloudBlobClient.getContainerReference(CONTAINER);
        Assertions.assertThrows(NoSuchElementException.class, () -> {
            container.listBlobs(BLOB_PREFIX).iterator().next();
        });
    }

    @Test
    public void whenContainerMissing_thenFileUploadFails() {
        Assertions.assertThrows(IOException.class, () -> {
            try (BlobOutputWriter writer = new BlobOutputWriter(
                    CLIENT_ID, ACCOUNT, "somenewcontainer",
                    BLOB_PREFIX, DataExtractorApplication.Output.JSON_LINES)) {
                OutputStream outputStream = writer.outputStream(cloudBlobClient);
                assertNotNull(outputStream);
                String filePath = "dataA1.json";
                InputStream inputStream = TestUtils.getStreamFromFile(filePath);
                IOUtils.copy(inputStream, outputStream);
                outputStream.flush();
                outputStream.close();
            }
        });
    }

    @Test
    public void whenAuthorisedClientAvailable_thenBlobStorageCanBeAccessed() throws Exception {
        String filePath = "dataA1.json";
        try (BlobOutputWriter writer = new BlobOutputWriter(
                CLIENT_ID, ACCOUNT, CONTAINER, BLOB_PREFIX, DataExtractorApplication.Output.JSON_LINES)) {

            // stub aad identity client
            BlobOutputWriter writerSpy = Mockito.spy(writer);
            Mockito.doReturn(cloudBlobClient).when(writerSpy).getClient();

            OutputStream outputStream = writerSpy.outputStream();
            assertNotNull(outputStream);
            InputStream inputStream = TestUtils.getStreamFromFile(filePath);
            IOUtils.copy(inputStream, outputStream);
        }
        // retrieve uploaded blob
        CloudBlobContainer container = cloudBlobClient.getContainerReference(CONTAINER);
        TestUtils.hasBlobThatStartsWith(container, BLOB_PREFIX);
    }

    @Test
    public void whenOutputStreamExists_thenSameInstanceIsReturned() throws Exception {
        try (BlobOutputWriter writer = new BlobOutputWriter(
                CLIENT_ID, ACCOUNT, CONTAINER, BLOB_PREFIX, DataExtractorApplication.Output.JSON_LINES)) {
            OutputStream outputStream = writer.outputStream(cloudBlobClient);
            assertNotNull(outputStream);
            OutputStream newOutputStream = writer.outputStream(cloudBlobClient);
            assertSame(outputStream, newOutputStream);
        }
    }

    @Test
    public void whenInvalidUriUsed_thenCannotGetClientInstance() throws Exception {
        Assertions.assertThrows(WriterException.class, () -> {
            try (BlobOutputWriter writer = new BlobOutputWriter(
                    CLIENT_ID, "someotheraccount", CONTAINER,
                    BLOB_PREFIX, DataExtractorApplication.Output.JSON_LINES)) {
                writer.getClient();
            }
        });
    }

    @Test
    public void whenAadNotAvailable_thenCannotGetCredentialsInstance() throws Exception {
        Assertions.assertThrows(WriterException.class, () -> {
            try (BlobOutputWriter writer = new BlobOutputWriter(
                    CLIENT_ID, "someotheraccount", CONTAINER,
                    BLOB_PREFIX, DataExtractorApplication.Output.JSON_LINES)) {
                writer.getCredentials();
            }
        });
    }

}
