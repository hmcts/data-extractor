package uk.gov.hmcts.reform.dataextractor;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.specialized.BlobOutputStream;
import com.azure.storage.blob.specialized.BlockBlobClient;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class ApiKeyStreamProviderTest {
    private static final String CONTAINER_NAME = "Container";
    private static final String FILE_NAME = "fileName";

    private ApiKeyStreamProvider classToTest;

    @Mock
    private BlobServiceClient mockClient;

    @Mock
    private BlobContainerClient blobContainerClientMock;

    @Mock
    private BlobClient blobMock;

    @Mock
    private BlockBlobClient blockBlobClientMock;

    @Mock
    private BlobOutputStream blockOutputStreamMock;

    @Before
    public void setup() {
        String connectionString = "";
        classToTest = spy(new ApiKeyStreamProvider(connectionString));
    }

    @Test
    public void givenNonExistingBlob_thenReturnBlockBlob() {
        DataExtractorApplication.Output type = DataExtractorApplication.Output.JSON_LINES;

        mockDependencies(true);

        assertThat(classToTest.getOutputStream(CONTAINER_NAME, FILE_NAME, type), is(blockOutputStreamMock));
    }

    @Test
    public void givenNonExistingContainer_thenReturnCreateContainer() {
        DataExtractorApplication.Output type = DataExtractorApplication.Output.JSON_LINES;

        mockDependencies(false);

        assertThat(classToTest.getOutputStream(CONTAINER_NAME, FILE_NAME, type), is(blockOutputStreamMock));
        verify(blobContainerClientMock, times(1)).create();
    }

    @Test(expected = WriterException.class)
    public void givenInvalidConnectionString_theWriterException() {
        DataExtractorApplication.Output type = DataExtractorApplication.Output.JSON_LINES;
        assertThat(classToTest.getOutputStream(CONTAINER_NAME, FILE_NAME, type), is(blockOutputStreamMock));
    }

    private void mockDependencies(boolean containerExist) {
        doReturn(mockClient).when(classToTest).getBlobServiceClient();
        doReturn(blobContainerClientMock).when(mockClient).getBlobContainerClient(CONTAINER_NAME);
        doReturn(containerExist).when(blobContainerClientMock).exists();
        doReturn(blobMock).when(blobContainerClientMock).getBlobClient(FILE_NAME);
        doReturn(blockBlobClientMock).when(blobMock).getBlockBlobClient();
        doReturn(blockOutputStreamMock).when(blockBlobClientMock).getBlobOutputStream();
    }
}
