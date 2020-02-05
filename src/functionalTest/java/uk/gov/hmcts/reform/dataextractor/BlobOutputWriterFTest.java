package uk.gov.hmcts.reform.dataextractor;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.microsoft.applicationinsights.core.dependencies.apachecommons.io.IOUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.util.ReflectionTestUtils;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import uk.gov.hmcts.reform.dataextractor.model.Output;
import uk.gov.hmcts.reform.dataextractor.service.impl.BlobServiceImpl;
import uk.gov.hmcts.reform.dataextractor.utils.TestUtils;
import uk.gov.hmcts.reform.mi.micore.factory.BlobServiceClientFactory;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static uk.gov.hmcts.reform.dataextractor.utils.TestUtils.hasBlobThatStartsWith;

@Testcontainers
@ExtendWith(MockitoExtension.class)
@ActiveProfiles(profiles = "test")
@SpringBootTest(classes = TestApplicationConfiguration.class)
public class BlobOutputWriterFTest {


    private static final String CLIENT_ID = "testclientid";
    private static final String CONTAINER = "testcontainer";
    private static final String BLOB_PREFIX = "testblob";
    private static final String ACCOUNT = "devstoreaccount1";

    @Autowired
    private BlobServiceImpl miStreamProvider;

    @Autowired
    private BlobServiceClientFactory blobServiceClientFactory;


    @Container
    public static final GenericContainer blobStorageContainer =
        new GenericContainer("arafato/azurite:2.6.5")
            .withEnv("executable", "blob")
            .withExposedPorts(10000);

    private BlobServiceClient testClient;

    @BeforeEach
    public void setUp() throws Exception {
        blobStorageContainer.start();
        Integer blobMappedPort = blobStorageContainer.getMappedPort(10000);
        String connString = "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;"
            + "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;"
            + "BlobEndpoint=http://127.0.0.1:" + blobMappedPort + "/devstoreaccount1;";

        ReflectionTestUtils.setField(miStreamProvider, "connectionString", connString);
        testClient = blobServiceClientFactory.getBlobClientWithConnectionString(connString);
    }

    @AfterEach
    public void tearDown() {
        blobStorageContainer.stop();
    }

    @Test
    public void whenBlobOutputWriterCreated_thenBufferedOutputAvailable() throws Exception {
        testClient.createBlobContainer(CONTAINER);
        miStreamProvider.getContainerLastUpdated(CONTAINER);
        try (BlobOutputWriter writer = new BlobOutputWriter(CONTAINER, BLOB_PREFIX,
            Output.JSON_LINES, miStreamProvider)) {
            OutputStream outputStream = writer.outputStream();
            assertThat(outputStream, instanceOf(BufferedOutputStream.class));
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"dataA1.json", "dataA1.csv"})
    public void whenFileUploaded_thenAvailableInBlobStorage(String filePath) throws Exception {
        testClient.createBlobContainer(CONTAINER);
        try (BlobOutputWriter writer = new BlobOutputWriter(CONTAINER, BLOB_PREFIX,
            Output.JSON_LINES, miStreamProvider)) {

            OutputStream outputStream = writer.outputStream();
            assertNotNull(outputStream);
            InputStream inputStream = TestUtils.getStreamFromFile(filePath);
            IOUtils.copy(inputStream, outputStream);
            outputStream.flush();
            outputStream.close();
        }
        // retrieve uploaded blob
        BlobContainerClient container = testClient.getBlobContainerClient(CONTAINER);
        assertTrue(hasBlobThatStartsWith(container, BLOB_PREFIX));
        BlobClient blob = TestUtils.downloadFirstBlobThatStartsWith(container, BLOB_PREFIX);
        assertTrue(blob.exists(), "Blob exist");
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        blob.download(outputStream);
        assertEquals(TestUtils.getDataFromFile(filePath), outputStream.toString());
    }

    //    @Test
    //    public void whenfileNotUploaded_thenMissingFromBlobStorage() throws Exception {
    //        testClient.createBlobContainer(CONTAINER);
    //        Assertions.assertThrows(NoSuchElementException.class, () -> {
    //            testClient.getBlobContainerClient(CONTAINER);
    //            miStreamProvider.getContainerLastUpdated()
    //            testClient.listBlobs(BLOB_PREFIX).iterator().next();
    //        });
    //    }
    //
    //    @Test
    //    public void whenContainerMissing_thenContainerIsCreated() throws Exception {
    //        String filePath = "dataA1.json";
    //        String containerName = "somenewcontainer";
    //
    //        try (BlobOutputWriter writer = new BlobOutputWriter(containerName,
    //                BLOB_PREFIX, Output.JSON_LINES, miStreamProvider)) {
    //
    //            OutputStream outputStream = writer.outputStream();
    //            assertNotNull(outputStream);
    //            InputStream inputStream = TestUtils.getStreamFromFile(filePath);
    //            IOUtils.copy(inputStream, outputStream);
    //            outputStream.flush();
    //            outputStream.close();
    //        }
    //        BlobContainerClient container = testClient.getBlobContainerClient(containerName);
    //        TestUtils.hasBlobThatStartsWith(container, BLOB_PREFIX);
    //        BlobClient blob = TestUtils.downloadFirstBlobThatStartsWith(container, BLOB_PREFIX);
    //        assertTrue(blob.exists());
    //        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    //        blob.download(outputStream);
    //        assertEquals(TestUtils.getDataFromFile(filePath), outputStream.toString());
    //
    //    }
    //
    //    @Test
    //    public void whenAuthorisedClientAvailable_thenBlobStorageCanBeAccessed() throws Exception {
    //        String filePath = "dataA1.json";
    //        try (BlobOutputWriter writer = new BlobOutputWriter(CONTAINER, BLOB_PREFIX,
    //            DataExtractorApplication.Output.JSON_LINES, miStreamProviderSpy)) {
    //
    //            BlobOutputWriter writerSpy = getSpyWriterWithMockClient(writer);
    //
    //            OutputStream outputStream = writerSpy.outputStream();
    //            assertNotNull(outputStream);
    //            InputStream inputStream = TestUtils.getStreamFromFile(filePath);
    //            IOUtils.copy(inputStream, outputStream);
    //        }
    //        // retrieve uploaded blob
    //        CloudBlobContainer container = cloudBlobClient.getContainerReference(CONTAINER);
    //        TestUtils.hasBlobThatStartsWith(container, BLOB_PREFIX);
    //    }
    //
    //    @Test
    //    public void whenOutputStreamExists_thenSameInstanceIsReturned() throws Exception {
    //        try (BlobOutputWriter writer = new BlobOutputWriter(CONTAINER, BLOB_PREFIX,
    //            DataExtractorApplication.Output.JSON_LINES, miStreamProviderSpy)) {
    //            BlobOutputWriter writerSpy = getSpyWriterWithMockClient(writer);
    //
    //            OutputStream outputStream = writerSpy.outputStream();
    //            assertNotNull(outputStream);
    //            OutputStream newOutputStream = writerSpy.outputStream();
    //            assertSame(outputStream, newOutputStream);
    //        }
    //    }

    //    private BlobOutputWriter getSpyWriterWithMockClient(BlobOutputWriter writer) throws Exception {
    //        BlobOutputWriter writerSpy = Mockito.spy(writer);
    //        when(writerSpy.getOutputStreamProvider()).thenReturn(miStreamProvider);
    //        return writerSpy;
    //    }


}
