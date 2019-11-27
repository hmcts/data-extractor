package uk.gov.hmcts.reform.dataextractor;

import com.microsoft.azure.storage.blob.BlobOutputStream;
import com.microsoft.azure.storage.blob.BlobProperties;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.net.URISyntaxException;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class ManageIdentityStreamProviderTest {

    private static final String CLIENT_ID = "testClientId";
    private static final String ACCOUNT_NAME = "testAccountName";
    private static final String CONTAINER_NAME = "Container";
    private static final String FILE_NAME = "fileName";
    private static final DataExtractorApplication.Output TYPE = DataExtractorApplication.Output.JSON_LINES;

    private ManageIdentityStreamProvider classToTest;

    @Mock
    private CloudBlobClient cloudBlobClientMock;
    @Mock
    private CloudBlobContainer cloudBlobContainerMock;
    @Mock
    private CloudBlockBlob cloudBlockBlobMock;
    @Mock
    private BlobProperties blobPropertiesMock;
    @Mock
    private BlobOutputStream blobOutputStreamMock;

    @Before
    public void setup() {
        classToTest = spy(new ManageIdentityStreamProvider(CLIENT_ID, ACCOUNT_NAME));
    }

    @Test
    public void givenExistingBlob_thenReturnOutputStream() throws Exception {
        mockDependencies(true);
        assertThat(classToTest.getOutputStream(CONTAINER_NAME, FILE_NAME, TYPE), is(blobOutputStreamMock));
        verify(cloudBlobContainerMock, never()).create();

    }

    @Test
    public void givenNonContainer_thenCreateContainer() throws Exception {
        mockDependencies(false);
        assertThat(classToTest.getOutputStream(CONTAINER_NAME, FILE_NAME, TYPE), is(blobOutputStreamMock));
        verify(cloudBlobContainerMock, times(1)).create();
    }

    @Test(expected = WriterException.class)
    public void givenError_whenGetClient_thenThrowError() throws Exception {
        doThrow(new WriterException(new Exception("error"))).when(classToTest).getCredentials();
        classToTest.getOutputStream(CONTAINER_NAME, FILE_NAME, TYPE);
    }

    @Test(expected = WriterException.class)
    public void givenUriException_whenGetClient_thenThrowError() throws Exception {
        doThrow(new URISyntaxException("error", "error")).when(classToTest).getClient();
        classToTest.getOutputStream(CONTAINER_NAME, FILE_NAME, TYPE);
    }


    private void mockDependencies(boolean containerExist) throws Exception {
        doReturn(cloudBlobClientMock).when(classToTest).getClient();
        doReturn(cloudBlobContainerMock).when(cloudBlobClientMock).getContainerReference(CONTAINER_NAME);
        doReturn(containerExist).when(cloudBlobContainerMock).exists();
        doReturn(cloudBlockBlobMock).when(cloudBlobContainerMock).getBlockBlobReference(FILE_NAME);
        doReturn(blobPropertiesMock).when(cloudBlockBlobMock).getProperties();
        doReturn(blobOutputStreamMock).when(cloudBlockBlobMock).openOutputStream();
    }
}
