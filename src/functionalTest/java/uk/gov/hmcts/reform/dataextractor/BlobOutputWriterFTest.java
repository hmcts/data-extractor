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
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;

@Testcontainers
@ExtendWith(MockitoExtension.class)
public class BlobOutputWriterFTest {

    private CloudBlobClient cloudBlobClient;

    private static final String CLIENT_ID = "testclientid";
    private static final String CONTAINER = "testcontainer";
    private static final String BLOB_PREFIX = "testblob";
    private static final String ACCOUNT = "devstoreaccount1";

    private final ManageIdentityStreamProvider miStreamProviderSpy =  Mockito.spy(new ManageIdentityStreamProvider(CLIENT_ID, ACCOUNT));

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
    public void whenBlobOutputWriterCreated_thenBufferedOutputAvailable() throws Exception {
        try (BlobOutputWriter writer = new BlobOutputWriter(CONTAINER, BLOB_PREFIX,
            DataExtractorApplication.Output.JSON_LINES, miStreamProviderSpy)) {
            BlobOutputWriter writerSpy = getSpyWriterWithMockClient(writer);

            OutputStream outputStream = writerSpy.outputStream();
            assertThat(outputStream, instanceOf(BufferedOutputStream.class));
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"dataA1.json", "dataA1.csv"})
    public void whenfileUploaded_thenAvailableInBlobStorage(String filePath) throws Exception {
        try (BlobOutputWriter writer = new BlobOutputWriter(CONTAINER, BLOB_PREFIX,
            DataExtractorApplication.Output.JSON_LINES, miStreamProviderSpy)) {

            BlobOutputWriter writerSpy = getSpyWriterWithMockClient(writer);

            OutputStream outputStream = writerSpy.outputStream();
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
    public void whenContainerMissing_thenContainerIsCreated() throws Exception {
        String filePath = "dataA1.json";
        String containerName = "somenewcontainer";
        try (BlobOutputWriter writer = new BlobOutputWriter(containerName,
                BLOB_PREFIX, DataExtractorApplication.Output.JSON_LINES, miStreamProviderSpy)) {

            BlobOutputWriter writerSpy = Mockito.spy(writer);
            ManageIdentityStreamProvider spyManageIdentityStreamProvider =  Mockito.spy(new ManageIdentityStreamProvider(CLIENT_ID, ACCOUNT));
            Mockito.spy(new ManageIdentityStreamProvider(CLIENT_ID, ACCOUNT));
            when(writerSpy.getOutputStreamProvider()).thenReturn(spyManageIdentityStreamProvider);
            doReturn(cloudBlobClient).when(spyManageIdentityStreamProvider).getClient();

            OutputStream outputStream = writerSpy.outputStream();
            assertNotNull(outputStream);
            InputStream inputStream = TestUtils.getStreamFromFile(filePath);
            IOUtils.copy(inputStream, outputStream);
            outputStream.flush();
            outputStream.close();
        }
        CloudBlobContainer container = cloudBlobClient.getContainerReference(containerName);
        TestUtils.hasBlobThatStartsWith(container, BLOB_PREFIX);
        CloudBlockBlob blob = TestUtils.downloadFirstBlobThatStartsWith(container, BLOB_PREFIX);
        assertTrue(blob.exists());
        assertEquals(TestUtils.getDataFromFile(filePath), blob.downloadText());

    }

    @Test
    public void whenAuthorisedClientAvailable_thenBlobStorageCanBeAccessed() throws Exception {
        String filePath = "dataA1.json";
        try (BlobOutputWriter writer = new BlobOutputWriter(CONTAINER, BLOB_PREFIX,
            DataExtractorApplication.Output.JSON_LINES, miStreamProviderSpy)) {

            BlobOutputWriter writerSpy = getSpyWriterWithMockClient(writer);

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
        try (BlobOutputWriter writer = new BlobOutputWriter(CONTAINER, BLOB_PREFIX,
            DataExtractorApplication.Output.JSON_LINES, miStreamProviderSpy)) {
            BlobOutputWriter writerSpy = getSpyWriterWithMockClient(writer);

            OutputStream outputStream = writerSpy.outputStream();
            assertNotNull(outputStream);
            OutputStream newOutputStream = writerSpy.outputStream();
            assertSame(outputStream, newOutputStream);
        }
    }
    
    private BlobOutputWriter getSpyWriterWithMockClient(BlobOutputWriter writer) throws Exception {
        BlobOutputWriter writerSpy = Mockito.spy(writer);
        when(writerSpy.getOutputStreamProvider()).thenReturn(miStreamProviderSpy);
        doReturn(cloudBlobClient).when(miStreamProviderSpy).getClient();
        return writerSpy;
    }


}
