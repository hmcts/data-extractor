package uk.gov.hmcts.reform.dataextractor.task;

import com.azure.core.http.rest.PagedIterable;
import com.azure.storage.blob.models.BlobContainerItem;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobItemProperties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import uk.gov.hmcts.reform.dataextractor.service.impl.BlobServiceImpl;
import uk.gov.hmcts.reform.dataextractor.test.utils.PagedIterableStub;

import java.time.LocalDate;
import java.time.OffsetDateTime;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class SetLastUpdateMetadataTaskTest {
    private static final String CONTAINER_1_NAME = "container1";
    private static final String CONTAINER_2_NAME = "container2";

    @Mock
    private BlobServiceImpl blobService;

    @InjectMocks
    private SetLastUpdateMetadataTask classToTest;

    @Test
    public void testDateContainerDateSet() {

        BlobContainerItem containerItem1 = new BlobContainerItem()
            .setName(CONTAINER_1_NAME);

        BlobContainerItem containerItem2 = new BlobContainerItem()
            .setName(CONTAINER_2_NAME);

        OffsetDateTime now = OffsetDateTime.now();

        BlobItem blobItem1 =  new BlobItem()
            .setProperties(new BlobItemProperties()
                .setLastModified(now.minusDays(7)))
            .setName("blobItem1");

        BlobItem blobItem2 = new BlobItem()
            .setProperties(new BlobItemProperties()
            .setLastModified(now.minusDays(2)))
            .setName("blobItem2");

        PagedIterable<BlobItem> blobs = new PagedIterableStub<>(blobItem1, blobItem2);
        PagedIterable<BlobContainerItem> stub = new PagedIterableStub<>(containerItem1, containerItem2);
        when(blobService.listContainers()).thenReturn(stub);
        when(blobService.listContainerBlobs(anyString())).thenReturn(blobs);

        classToTest.execute();

        verify(blobService, times(1)).setLastUpdated(CONTAINER_1_NAME, now.minusDays(2).toLocalDate());
        verify(blobService, times(1)).setLastUpdated(CONTAINER_2_NAME, now.minusDays(2).toLocalDate());
    }

    @Test
    public void givenEmptyContainer_thenUpdateSkipped() {

        BlobContainerItem containerItem1 = new BlobContainerItem()
            .setName(CONTAINER_1_NAME);

        PagedIterable<BlobItem> blobs = new PagedIterableStub<>();
        PagedIterable<BlobContainerItem> stub = new PagedIterableStub<>(containerItem1);
        when(blobService.listContainers()).thenReturn(stub);
        when(blobService.listContainerBlobs(anyString())).thenReturn(blobs);

        classToTest.execute();

        verify(blobService, never()).setLastUpdated(eq(CONTAINER_1_NAME), any(LocalDate.class));
    }
}
