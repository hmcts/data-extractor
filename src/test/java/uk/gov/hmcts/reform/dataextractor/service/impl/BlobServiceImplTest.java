package uk.gov.hmcts.reform.dataextractor.service.impl;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.models.BlobContainerProperties;
import com.azure.storage.blob.models.BlobHttpHeaders;
import com.azure.storage.blob.models.ParallelTransferOptions;
import com.azure.storage.blob.specialized.BlobOutputStream;
import com.azure.storage.blob.specialized.BlockBlobClient;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import uk.gov.hmcts.reform.dataextractor.model.Output;
import uk.gov.hmcts.reform.dataextractor.test.utils.Matchers;
import uk.gov.hmcts.reform.mi.micore.factory.BlobServiceClientFactory;

import java.time.LocalDate;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static uk.gov.hmcts.reform.dataextractor.service.ContainerConstants.UPDATE_DATE_METADATA;
import static uk.gov.hmcts.reform.dataextractor.service.impl.BlobServiceImpl.DATE_TIME_FORMATTER;

@ExtendWith(MockitoExtension.class)
public class BlobServiceImplTest {

    private static final String STORAGE_ACCOUNT = "testStorageAccount";
    private static final String CLIENT_ID = "testClientId";
    private static final String CONNECTION_STRING = "testStorageConnectionString";
    private static final String TEST_CONTAINER_NAME = "testContainer";


    private BlobServiceImpl classToTest;

    @Mock
    private BlobServiceClient blobServiceClientMock;
    @Mock
    private BlobServiceClientFactory blobServiceClientFactory;
    @Mock
    private BlobContainerClient blobContainerClientMock;
    @Mock
    private BlobClient blobClientMock;
    @Mock
    private BlockBlobClient blockBlobClientMock;
    @Mock
    private BlobOutputStream outputStream;

    @Test
    public void givenNonExistingContainer_whenGetLastUpdated_thenReturnNull() {
        classToTest = new BlobServiceImpl(CLIENT_ID, CONNECTION_STRING, STORAGE_ACCOUNT, blobServiceClientFactory);
        when(blobServiceClientFactory.getBlobClientWithManagedIdentity(CLIENT_ID, STORAGE_ACCOUNT)).thenReturn(
            blobServiceClientMock);

        when(blobServiceClientMock.getBlobContainerClient(TEST_CONTAINER_NAME)).thenReturn(blobContainerClientMock);
        when(blobContainerClientMock.exists()).thenReturn(false);
        assertNull(classToTest.getContainerLastUpdated(TEST_CONTAINER_NAME), "Expected null");
    }

    @Test
    public void givenNullMetadata_whenGetLastUpdated_thenReturnNull() {
        classToTest = new BlobServiceImpl(CLIENT_ID, CONNECTION_STRING, STORAGE_ACCOUNT, blobServiceClientFactory);
        when(blobServiceClientFactory.getBlobClientWithManagedIdentity(CLIENT_ID, STORAGE_ACCOUNT)).thenReturn(
            blobServiceClientMock);
        when(blobServiceClientMock.getBlobContainerClient(TEST_CONTAINER_NAME)).thenReturn(blobContainerClientMock);
        when(blobContainerClientMock.exists()).thenReturn(true);
        when(blobContainerClientMock.getProperties()).thenReturn(createEmptyBlobContainerProperties());
        assertNull(classToTest.getContainerLastUpdated(TEST_CONTAINER_NAME), "Expected null");
    }

    @Test
    public void givenValidMetadata_whenGetLastUpdated_thenReturnMetadataDate() {
        classToTest = new BlobServiceImpl(CLIENT_ID, CONNECTION_STRING, STORAGE_ACCOUNT, blobServiceClientFactory);

        final LocalDate expectedDate = LocalDate.now();

        when(blobServiceClientFactory.getBlobClientWithManagedIdentity(CLIENT_ID, STORAGE_ACCOUNT)).thenReturn(
            blobServiceClientMock);
        when(blobServiceClientMock.getBlobContainerClient(TEST_CONTAINER_NAME)).thenReturn(blobContainerClientMock);
        when(blobContainerClientMock.exists()).thenReturn(true);
        when(blobContainerClientMock.getProperties()).thenReturn(createBlobContainerPrWithMetadata(expectedDate.format(DATE_TIME_FORMATTER)));
        assertEquals(classToTest.getContainerLastUpdated(TEST_CONTAINER_NAME), expectedDate, "Expected date");
    }

    @Test
    public void whenSetLastUpdated_thenApiIsCalled() {
        classToTest = new BlobServiceImpl(CLIENT_ID, CONNECTION_STRING, STORAGE_ACCOUNT, blobServiceClientFactory);

        final String dateMetadata = DATE_TIME_FORMATTER.format(LocalDate.now());
        when(blobServiceClientFactory.getBlobClientWithManagedIdentity(CLIENT_ID, STORAGE_ACCOUNT)).thenReturn(
            blobServiceClientMock);
        when(blobServiceClientMock.getBlobContainerClient(TEST_CONTAINER_NAME)).thenReturn(blobContainerClientMock);
        classToTest.setLastUpdated(TEST_CONTAINER_NAME, LocalDate.now());
        verify(blobContainerClientMock, times(1)).setMetadata(Map.of(UPDATE_DATE_METADATA, dateMetadata));
    }

    @Test
    public void whenHasClientId_thenUseManageIdentityClient() throws Exception {
        classToTest = new BlobServiceImpl(CLIENT_ID, CONNECTION_STRING, STORAGE_ACCOUNT, blobServiceClientFactory);

        when(blobServiceClientFactory.getBlobClientWithManagedIdentity(CLIENT_ID, STORAGE_ACCOUNT)).thenReturn(
            blobServiceClientMock);

        assertEquals(classToTest.getConnection(), blobServiceClientMock, "Verify blob service client");
        verify(blobServiceClientFactory, times(1)).getBlobClientWithManagedIdentity(CLIENT_ID, STORAGE_ACCOUNT);
    }

    @Test
    public void whenEmptyClientId_thenUseConnectionStringClient() throws Exception {
        classToTest = new BlobServiceImpl(StringUtils.EMPTY, CONNECTION_STRING, STORAGE_ACCOUNT, blobServiceClientFactory);
        when(blobServiceClientFactory.getBlobClientWithConnectionString(CONNECTION_STRING)).thenReturn(
            blobServiceClientMock);

        assertEquals(classToTest.getConnection(), blobServiceClientMock, "Verify blob service client");
        verify(blobServiceClientFactory, times(1)).getBlobClientWithConnectionString(CONNECTION_STRING);
    }

    @Test
    public void createBlobWithContentType() {
        classToTest = new BlobServiceImpl(StringUtils.EMPTY, CONNECTION_STRING, STORAGE_ACCOUNT, blobServiceClientFactory);
        String fileName = "testFileName.jsonline";
        when(blobServiceClientFactory.getBlobClientWithConnectionString(CONNECTION_STRING)).thenReturn(
            blobServiceClientMock);


        when(blobServiceClientMock.getBlobContainerClient(TEST_CONTAINER_NAME)).thenReturn(blobContainerClientMock);
        when(blobContainerClientMock.getBlobClient(fileName)).thenReturn(blobClientMock);
        when(blobClientMock.getBlockBlobClient()).thenReturn(blockBlobClientMock);

        BlobHttpHeaders blobHeaders = new BlobHttpHeaders()
            .setContentType(Output.JSON_LINES.getApplicationContent());

        when(blockBlobClientMock.getBlobOutputStream(any(ParallelTransferOptions.class), Matchers.argsThat(blobHeaders),
            isNull(), isNull(), isNull())).thenReturn(outputStream);

        assertEquals(classToTest.getOutputStream(TEST_CONTAINER_NAME, fileName, Output.JSON_LINES), outputStream, "Validate outputStream");
    }

    private BlobContainerProperties createEmptyBlobContainerProperties() {
        return new BlobContainerProperties(null, null, null, null,
            null, null, null, false, false);
    }

    private BlobContainerProperties createBlobContainerPrWithMetadata(String metadataValue) {
        return new BlobContainerProperties(Map.of(UPDATE_DATE_METADATA, metadataValue), null, null, null,
            null, null, null, false, false);
    }
}
