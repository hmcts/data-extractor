package uk.gov.hmcts.reform.dataextractor;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.context.ConfigFileApplicationContextInitializer;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.test.context.junit4.SpringRunner;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.net.URISyntaxException;
import java.security.InvalidKeyException;

import static org.mockito.Mockito.spy;


@Testcontainers
@SpringBootTest
@ExtendWith(SpringExtension.class)
@ContextConfiguration(classes = DataExtractorApplication.class)
@ActiveProfiles("test")
public class SomeTest extends  DbTest{
    private CloudBlobClient cloudBlobClient;

    private static final String CLIENT_ID = "testclientid";
    private static final String CONTAINER = "testcontainer";
    private static final String BLOB_PREFIX = "testblob";
    private static final String ACCOUNT = "devstoreaccount1";

    private final ManageIdentityStreamProvider miStreamProviderSpy =  spy(new ManageIdentityStreamProvider(CLIENT_ID, ACCOUNT));

    @BeforeAll
    public static void all() {
        System.setProperty("DB_URL",SomeTest.postgresContainer.getJdbcUrl());
        System.out.println();
    }

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
    public void test(){
        System.out.println("Something");
    }

}
