package uk.gov.hmcts.reform.dataextractor;

import com.azure.core.http.rest.PagedIterable;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.models.BlobContainerItem;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.test.util.ReflectionTestUtils;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import uk.gov.hmcts.reform.dataextractor.config.DbConfig;
import uk.gov.hmcts.reform.dataextractor.service.impl.BlobServiceImpl;
import uk.gov.hmcts.reform.dataextractor.utils.TestUtils;
import uk.gov.hmcts.reform.mi.micore.factory.BlobServiceClientFactory;

import java.io.ByteArrayOutputStream;
import java.util.Map;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static uk.gov.hmcts.reform.dataextractor.service.ContainerConstants.UPDATE_DATE_METADATA;
import static uk.gov.hmcts.reform.dataextractor.utils.TestConstants.AZURE_TEST_CONTAINER_IMAGE;
import static uk.gov.hmcts.reform.dataextractor.utils.TestConstants.DEFAULT_COMMAND;

@Testcontainers
@ExtendWith(SpringExtension.class)
@ActiveProfiles(profiles = "test")
@SpringBootTest(classes = TestApplicationConfiguration.class)
public class ExtractionComponentFTest extends DbTest {

    private static final String TEST_CONTAINER_NAME = "test-container";
    private static final String BLOB_NAME_PREFIX = "JLines";
    @Container
    public static final GenericContainer blobStorageContainer =
        new GenericContainer(AZURE_TEST_CONTAINER_IMAGE)
            .withEnv("executable", "blob")
            .withCommand(DEFAULT_COMMAND)
            .withExposedPorts(10000);

    @Autowired
    private ExtractionComponent extractionComponent;

    @Autowired
    private BlobServiceImpl blobService;

    @Autowired
    private DbConfig dbConfig;

    @Autowired
    private BlobServiceClientFactory blobServiceClientFactory;

    private BlobServiceClient testClient;

    @BeforeEach
    public void setUp() {
        blobStorageContainer.start();
        Integer blobMappedPort = blobStorageContainer.getMappedPort(10000);

        String connString = "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;"
            + "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;"
            + "BlobEndpoint=http://127.0.0.1:" + blobMappedPort + "/devstoreaccount1;";

        ReflectionTestUtils.setField(blobService, "connectionString",  connString);

        ReflectionTestUtils.setField(dbConfig, "url",  jdbcUrl);
        ReflectionTestUtils.setField(dbConfig, "user",  username);
        ReflectionTestUtils.setField(dbConfig, "password",  password);
        testClient = blobServiceClientFactory.getBlobClientWithConnectionString(connString);
    }

    @AfterEach
    public void tearDown() {
        blobStorageContainer.stop();
    }

    @Test
    public void givenContainerWithMetadataExecution_thenExtractFilteredData() {
        BlobContainerClient containerClient = testClient.createBlobContainer(TEST_CONTAINER_NAME);
        containerClient.setMetadata(Map.of(UPDATE_DATE_METADATA, "20200101"));

        extractionComponent.execute();

        PagedIterable<BlobContainerItem> containers = testClient.listBlobContainers();
        testClient.getBlobContainerClient(TEST_CONTAINER_NAME);
        assertTrue(containers.stream().map(BlobContainerItem::getName).anyMatch(TEST_CONTAINER_NAME::equals));

        BlobClient blob = TestUtils.downloadFirstBlobThatStartsWith(containerClient, BLOB_NAME_PREFIX);
        assertTrue(blob.exists(), "Blob exist");

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        blob.download(outputStream);

        assertTrue(Pattern.compile(TestUtils.getDataFromFile("filtered-data.jsonl"))
            .matcher(outputStream.toString())
            .matches(),"Expected output");
    }

    @Test
    public void givenInitialExecution_thenExtractAllData() {

        extractionComponent.execute();
        BlobContainerClient containerClient = testClient.getBlobContainerClient(TEST_CONTAINER_NAME);
        assertTrue(containerClient.exists());

        BlobClient blob = TestUtils.downloadFirstBlobThatStartsWith(containerClient, BLOB_NAME_PREFIX);
        assertTrue(blob.exists(), "Blob exist");
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        blob.download(outputStream);
        assertTrue(Pattern.compile(TestUtils.getDataFromFile("data/all-data-part1.jsonl"))
            .matcher(outputStream.toString())
            .matches(),"Expected output");

        blob.delete();
        blob = TestUtils.downloadFirstBlobThatStartsWith(containerClient, BLOB_NAME_PREFIX);
        assertTrue(blob.exists(), "Blob exist");


        outputStream = new ByteArrayOutputStream();
        blob.download(outputStream);
        assertTrue(Pattern.compile(TestUtils.getDataFromFile("data/all-data-part2.jsonl"))
            .matcher(outputStream.toString())
            .matches(),"Expected output");

    }

}
