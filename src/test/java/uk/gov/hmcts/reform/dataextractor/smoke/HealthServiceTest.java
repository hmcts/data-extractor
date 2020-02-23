package uk.gov.hmcts.reform.dataextractor.smoke;

import com.azure.storage.blob.models.BlobContainerItem;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import uk.gov.hmcts.reform.dataextractor.service.CaseDataService;
import uk.gov.hmcts.reform.dataextractor.service.impl.BlobServiceImpl;
import uk.gov.hmcts.reform.dataextractor.test.utils.PagedIterableStub;
import uk.gov.hmcts.reform.mi.micore.exception.ServiceNotAvailableException;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class HealthServiceTest {

    @InjectMocks
    private HealthService classToTest;
    @Mock
    private BlobServiceImpl blobService;
    @Mock
    private CaseDataService caseDataService;
    @Mock
    private BlobContainerItem containerItem;

    @Test
    public void testCheckAllDependencies() throws ServiceNotAvailableException {
        when(blobService.listContainers()).thenReturn(new PagedIterableStub<>(containerItem));

        classToTest.check();
        verify(blobService, times(1)).listContainers();
        verify(caseDataService, times(1)).checkConnection();
    }

    @Test
    public void testExceptionOnDependencyFail() {
        doThrow(new RuntimeException()).when(blobService).listContainers();
        assertThrows(ServiceNotAvailableException.class, () -> classToTest.check());
    }
}
