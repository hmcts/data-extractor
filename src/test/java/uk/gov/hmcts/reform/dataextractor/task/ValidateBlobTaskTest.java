package uk.gov.hmcts.reform.dataextractor.task;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import com.azure.core.http.rest.PagedIterable;
import com.azure.storage.blob.models.BlobContainerItem;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobItemProperties;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.LoggerFactory;

import uk.gov.hmcts.reform.dataextractor.model.Output;
import uk.gov.hmcts.reform.dataextractor.service.impl.BlobServiceImpl;
import uk.gov.hmcts.reform.dataextractor.test.utils.PagedIterableStub;
import uk.gov.hmcts.reform.dataextractor.test.utils.logger.TestAppender;
import uk.gov.hmcts.reform.dataextractor.test.utils.logger.TestRecorder;

import java.time.OffsetDateTime;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ValidateBlobTaskTest {
    private static final String CONTAINER_1_NAME = "container1";
    private static final String CONTAINER_2_NAME = "container2";

    private ValidateBlobTask classToTest;
    static TestAppender logAppender = new TestAppender();

    @Mock
    private BlobServiceImpl blobService;
    @Mock
    private TestRecorder testRecorder;

    @BeforeAll
    public static void loggerConfig() {
        LoggerContext logCtx = (LoggerContext) LoggerFactory.getILoggerFactory();
        Logger log = logCtx.getLogger(ValidateBlobTask.class.getName());
        log.addAppender(logAppender);
    }


    @AfterAll
    public static void tearDownLogger() {
        logAppender.stop();
    }

    @BeforeEach
    public void setUp() {
        logAppender.setRecorder(testRecorder);
    }

    @AfterEach
    public void tearDown() {
        logAppender.clean();
    }

    @Test
    void executeWhenEnabled() {


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

        when(blobService.validateBlob(CONTAINER_1_NAME, blobItem1.getName(), Output.JSON)).thenReturn(true);
        classToTest = new ValidateBlobTask(blobService, true);
        classToTest.execute();
        verify(testRecorder, times(1)).append("[WARN] Blob in container2 with name blobItem2 not valid");
        verify(testRecorder, times(1)).append("[WARN] Blob in container1 with name blobItem2 not valid");
        verify(testRecorder, times(1)).append("[INFO] Blob in container1 with name blobItem1 is correct");
    }

    @Test
    void executeWhenDisabled() {
        classToTest = new ValidateBlobTask(blobService, false);
        classToTest.execute();
        verifyNoInteractions(blobService);
    }
}