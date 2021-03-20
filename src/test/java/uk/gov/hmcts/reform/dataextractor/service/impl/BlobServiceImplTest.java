package uk.gov.hmcts.reform.dataextractor.service.impl;

import com.azure.core.http.rest.PagedIterable;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.models.BlobContainerItem;
import com.azure.storage.blob.models.BlobContainerProperties;
import com.azure.storage.blob.models.BlobHttpHeaders;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.ParallelTransferOptions;
import com.azure.storage.blob.specialized.BlobInputStream;
import com.azure.storage.blob.specialized.BlobOutputStream;
import com.azure.storage.blob.specialized.BlockBlobClient;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import uk.gov.hmcts.reform.dataextractor.Factory;
import uk.gov.hmcts.reform.dataextractor.model.Output;
import uk.gov.hmcts.reform.dataextractor.service.BlobOutputValidator;
import uk.gov.hmcts.reform.dataextractor.test.utils.Matchers;
import uk.gov.hmcts.reform.dataextractor.test.utils.PagedIterableStub;
import uk.gov.hmcts.reform.mi.micore.factory.BlobServiceClientFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.LocalDate;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static uk.gov.hmcts.reform.dataextractor.service.ContainerConstants.UPDATE_DATE_METADATA;
import static uk.gov.hmcts.reform.dataextractor.service.impl.BlobServiceImpl.DATE_TIME_FORMATTER;

@SuppressWarnings({"PMD.ExcessiveImports", "PMD.TooManyMethods", "PMD.LawOfDemeter"})
@ExtendWith(MockitoExtension.class)
class BlobServiceImplTest {

    private static final String INVALID_BLOB_ASSERT_MESSAGE = "Expected invalid blob";
    private static final String VALID_BLOB_ASSERT_MESSAGE = "Expected valid blob";

    private static final String STORAGE_ACCOUNT = "testStorageAccount";
    private static final String CLIENT_ID = "testClientId";
    private static final String CONNECTION_STRING = "testStorageConnectionString";
    private static final String TEST_CONTAINER_NAME = "testContainer";
    private static final String BLOB_NAME = "blobName";

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
    @Mock
    private BlobOutputValidator blobOutputValidator;
    @Mock
    private BlobInputStream blobInputStream;
    @Mock
    private Factory<Output, BlobOutputValidator> blobOutputValidatorFactory;

    @Test
    void givenNonExistingContainer_whenGetLastUpdated_thenReturnNull() {
        classToTest = new BlobServiceImpl(CLIENT_ID, CONNECTION_STRING, STORAGE_ACCOUNT, blobServiceClientFactory, blobOutputValidatorFactory);
        when(blobServiceClientFactory.getBlobClientWithManagedIdentity(CLIENT_ID, STORAGE_ACCOUNT)).thenReturn(
            blobServiceClientMock);

        when(blobServiceClientMock.getBlobContainerClient(TEST_CONTAINER_NAME)).thenReturn(blobContainerClientMock);
        when(blobContainerClientMock.exists()).thenReturn(false);
        assertNull(classToTest.getContainerLastUpdated(TEST_CONTAINER_NAME, true), "Expected null");
    }

    @Test
    void givenNullMetadata_whenGetLastUpdated_thenReturnNull() {
        classToTest = new BlobServiceImpl(CLIENT_ID, CONNECTION_STRING, STORAGE_ACCOUNT, blobServiceClientFactory, blobOutputValidatorFactory);
        when(blobServiceClientFactory.getBlobClientWithManagedIdentity(CLIENT_ID, STORAGE_ACCOUNT)).thenReturn(
            blobServiceClientMock);
        when(blobServiceClientMock.getBlobContainerClient(TEST_CONTAINER_NAME)).thenReturn(blobContainerClientMock);
        when(blobContainerClientMock.exists()).thenReturn(true);
        when(blobContainerClientMock.getProperties()).thenReturn(createEmptyBlobContainerProperties());
        assertNull(classToTest.getContainerLastUpdated(TEST_CONTAINER_NAME, true), "Expected null");
    }

    @Test
    void givenValidMetadata_whenGetLastUpdated_thenReturnMetadataDate() {
        classToTest = new BlobServiceImpl(CLIENT_ID, CONNECTION_STRING, STORAGE_ACCOUNT, blobServiceClientFactory, blobOutputValidatorFactory);

        final LocalDate expectedDate = LocalDate.now();

        when(blobServiceClientFactory.getBlobClientWithManagedIdentity(CLIENT_ID, STORAGE_ACCOUNT)).thenReturn(
            blobServiceClientMock);
        when(blobServiceClientMock.getBlobContainerClient(TEST_CONTAINER_NAME)).thenReturn(blobContainerClientMock);
        when(blobContainerClientMock.exists()).thenReturn(true);
        when(blobContainerClientMock.getProperties()).thenReturn(createBlobContainerPrWithMetadata(expectedDate.format(DATE_TIME_FORMATTER)));
        assertEquals(classToTest.getContainerLastUpdated(TEST_CONTAINER_NAME, true), expectedDate, "Expected date");
    }

    @Test
    void whenSetLastUpdated_thenApiIsCalled() {
        classToTest = new BlobServiceImpl(CLIENT_ID, CONNECTION_STRING, STORAGE_ACCOUNT, blobServiceClientFactory, blobOutputValidatorFactory);

        final String dateMetadata = DATE_TIME_FORMATTER.format(LocalDate.now());
        when(blobServiceClientFactory.getBlobClientWithManagedIdentity(CLIENT_ID, STORAGE_ACCOUNT)).thenReturn(
            blobServiceClientMock);
        when(blobServiceClientMock.getBlobContainerClient(TEST_CONTAINER_NAME)).thenReturn(blobContainerClientMock);
        classToTest.setLastUpdated(TEST_CONTAINER_NAME, LocalDate.now());
        verify(blobContainerClientMock, times(1)).setMetadata(Map.of(UPDATE_DATE_METADATA, dateMetadata));
    }

    @Test
    void whenHasClientId_thenUseManageIdentityClient() throws Exception {
        classToTest = new BlobServiceImpl(CLIENT_ID, CONNECTION_STRING, STORAGE_ACCOUNT, blobServiceClientFactory, blobOutputValidatorFactory);

        when(blobServiceClientFactory.getBlobClientWithManagedIdentity(CLIENT_ID, STORAGE_ACCOUNT)).thenReturn(
            blobServiceClientMock);

        assertEquals(classToTest.getConnection(), blobServiceClientMock, "Verify blob service client");
        verify(blobServiceClientFactory, times(1)).getBlobClientWithManagedIdentity(CLIENT_ID, STORAGE_ACCOUNT);
    }

    @Test
    void whenEmptyClientId_thenUseConnectionStringClient() throws Exception {
        classToTest = new BlobServiceImpl(StringUtils.EMPTY, CONNECTION_STRING, STORAGE_ACCOUNT, blobServiceClientFactory,
            blobOutputValidatorFactory);
        when(blobServiceClientFactory.getBlobClientWithConnectionString(CONNECTION_STRING)).thenReturn(
            blobServiceClientMock);

        assertEquals(classToTest.getConnection(), blobServiceClientMock, "Verify blob service client");
        verify(blobServiceClientFactory, times(1)).getBlobClientWithConnectionString(CONNECTION_STRING);
    }

    @Test
    void createBlobWithContentType() {
        classToTest = new BlobServiceImpl(StringUtils.EMPTY, CONNECTION_STRING, STORAGE_ACCOUNT, blobServiceClientFactory,
            blobOutputValidatorFactory);
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

    @Test
    void testListContainers() {
        classToTest = new BlobServiceImpl(StringUtils.EMPTY, CONNECTION_STRING, STORAGE_ACCOUNT, blobServiceClientFactory,
            blobOutputValidatorFactory);
        when(blobServiceClientFactory.getBlobClientWithConnectionString(CONNECTION_STRING)).thenReturn(
            blobServiceClientMock);
        PagedIterable<BlobContainerItem> expected = new PagedIterableStub<>();
        when(blobServiceClientMock.listBlobContainers()).thenReturn(expected);
        assertEquals(classToTest.listContainers(), expected, "Expected container list");
    }

    @Test
    void testListBlobs() {
        classToTest = new BlobServiceImpl(StringUtils.EMPTY, CONNECTION_STRING, STORAGE_ACCOUNT, blobServiceClientFactory,
            blobOutputValidatorFactory);
        when(blobServiceClientFactory.getBlobClientWithConnectionString(CONNECTION_STRING)).thenReturn(
            blobServiceClientMock);
        PagedIterable<BlobItem> expected = new PagedIterableStub<>();
        when(blobServiceClientMock.getBlobContainerClient(TEST_CONTAINER_NAME)).thenReturn(blobContainerClientMock);
        when(blobContainerClientMock.listBlobs()).thenReturn(expected);
        assertEquals(classToTest.listContainerBlobs(TEST_CONTAINER_NAME), expected, "Expected container blob list");
    }

    @Test
    void givenException_whenValidateBlob_thenReturnFalse() throws IOException {
        classToTest = spy(new BlobServiceImpl(StringUtils.EMPTY, CONNECTION_STRING, STORAGE_ACCOUNT, blobServiceClientFactory,
            blobOutputValidatorFactory));
        when(blobOutputValidatorFactory.provide(Output.JSON_LINES)).thenReturn(blobOutputValidator);
        when(blobServiceClientFactory.getBlobClientWithConnectionString(CONNECTION_STRING)).thenReturn(
            blobServiceClientMock);
        when(blobServiceClientMock.getBlobContainerClient(TEST_CONTAINER_NAME)).thenReturn(blobContainerClientMock);
        when(blobContainerClientMock.getBlobClient(BLOB_NAME)).thenReturn(blobClientMock);
        when(blobClientMock.openInputStream()).thenReturn(blobInputStream);
        doThrow(new IOException("Test error")).when(blobInputStream).read(any(), anyInt(), anyInt());
        assertFalse(classToTest.validateBlob(TEST_CONTAINER_NAME, BLOB_NAME, Output.JSON_LINES), INVALID_BLOB_ASSERT_MESSAGE);
        verify(blobInputStream, times(1)).close();

    }

    @Test
    void givenValidBlob_whenValidateBlob_thenReturnTrue() throws IOException {
        classToTest = spy(new BlobServiceImpl(StringUtils.EMPTY, CONNECTION_STRING, STORAGE_ACCOUNT, blobServiceClientFactory,
            blobOutputValidatorFactory));
        try (InputStream inputStream = spy(new ByteArrayInputStream("{}\n{}".getBytes()))) {
            doReturn(inputStream).when(classToTest).getInputStream(eq(TEST_CONTAINER_NAME), anyString());
            when(blobOutputValidatorFactory.provide(Output.JSON_LINES)).thenReturn(blobOutputValidator);
            assertTrue(classToTest.validateBlob(TEST_CONTAINER_NAME, BLOB_NAME, Output.JSON_LINES), VALID_BLOB_ASSERT_MESSAGE);
            verify(inputStream, times(1)).close();
            verify(blobOutputValidator, times(2)).isNotValid("{}");
        }
    }

    @Test
    void givenInvalidBlob_whenValidateBlob_thenReturnFalse() throws IOException {
        classToTest = spy(new BlobServiceImpl(StringUtils.EMPTY, CONNECTION_STRING, STORAGE_ACCOUNT, blobServiceClientFactory,
            blobOutputValidatorFactory));
        try (InputStream inputStream = spy(new ByteArrayInputStream("{}\n{}".getBytes()))) {
            when(blobOutputValidatorFactory.provide(Output.JSON_LINES)).thenReturn(blobOutputValidator);
            doReturn(inputStream).when(classToTest).getInputStream(eq(TEST_CONTAINER_NAME), anyString());
            when(blobOutputValidator.isNotValid("{}")).thenReturn(true);
            assertFalse(classToTest.validateBlob(TEST_CONTAINER_NAME, BLOB_NAME, Output.JSON_LINES), INVALID_BLOB_ASSERT_MESSAGE);
            verify(inputStream, times(1)).close();
            verify(blobOutputValidator, times(1)).isNotValid("{}");
        }
    }

    @Test
    void testDeleteBlob() {
        classToTest = new BlobServiceImpl(StringUtils.EMPTY, CONNECTION_STRING, STORAGE_ACCOUNT, blobServiceClientFactory,
            blobOutputValidatorFactory);
        when(blobServiceClientFactory.getBlobClientWithConnectionString(CONNECTION_STRING)).thenReturn(
            blobServiceClientMock);
        when(blobServiceClientMock.getBlobContainerClient(TEST_CONTAINER_NAME)).thenReturn(blobContainerClientMock);
        when(blobContainerClientMock.getBlobClient(BLOB_NAME)).thenReturn(blobClientMock);
        classToTest.deleteBlob(TEST_CONTAINER_NAME, BLOB_NAME);
        verify(blobClientMock, times(1)).delete();
    }

    @Test
    void givenInitialiseFlagFalse_whenGetContainerLastUpdate_thenContainerNotCreated() {
        classToTest = new BlobServiceImpl(StringUtils.EMPTY, CONNECTION_STRING, STORAGE_ACCOUNT, blobServiceClientFactory,
            blobOutputValidatorFactory);
        when(blobServiceClientFactory.getBlobClientWithConnectionString(CONNECTION_STRING)).thenReturn(
            blobServiceClientMock);
        when(blobServiceClientMock.getBlobContainerClient(TEST_CONTAINER_NAME)).thenReturn(blobContainerClientMock);
        when(blobContainerClientMock.exists()).thenReturn(false);
        assertNull(classToTest.getContainerLastUpdated(TEST_CONTAINER_NAME, false), "Expected container metadata not set");
        verify(blobContainerClientMock, never()).create();
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
