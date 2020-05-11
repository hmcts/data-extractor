package uk.gov.hmcts.reform.dataextractor;

import com.google.common.collect.ImmutableMap;
import com.microsoft.applicationinsights.TelemetryClient;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.context.ApplicationContext;
import org.springframework.test.util.ReflectionTestUtils;

import uk.gov.hmcts.reform.dataextractor.task.PreExecutor;
import uk.gov.hmcts.reform.dataextractor.task.SetLastUpdateMetadataTask;
import uk.gov.hmcts.reform.mi.micore.component.HealthCheck;
import uk.gov.hmcts.reform.mi.micore.exception.ServiceNotAvailableException;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class DataExtractorApplicationTest {

    @InjectMocks
    private DataExtractorApplication classToTest;

    @Mock
    private ExtractionComponent extractionComponent;

    @Mock
    private HealthCheck healthCheck;

    @Mock
    private TelemetryClient client;

    @Mock
    private ApplicationContext contextMock;

    @Mock
    private PreExecutor preExecutorMock;

    @Test
    public void testApplicationExecuted() throws Exception {
        classToTest.run(null);
        verify(extractionComponent, times(1)).execute();
        verify(healthCheck, never()).check();
        verify(client, times(1)).flush();
    }

    @Test
    public void testSmokeCheckExecuted() throws Exception {
        ReflectionTestUtils.setField(classToTest, "smokeTest", true);
        classToTest.run(null);
        verify(healthCheck, times(1)).check();
        verify(extractionComponent, never()).execute();
        verify(client, times(1)).flush();
    }

    @Test
    public void testPreExecutionApplicationExecuted() throws Exception {
        SetLastUpdateMetadataTask disabledExecutorMock = spy(new SetLastUpdateMetadataTask());

        when(contextMock.getBeansOfType(PreExecutor.class)).thenReturn(ImmutableMap
            .of("preExecutorMock", preExecutorMock,
                "disabledExecutorMock", disabledExecutorMock));
        when(preExecutorMock.isEnabled()).thenReturn(true);
        classToTest.run(null);
        verify(extractionComponent, times(1)).execute();
        verify(healthCheck, never()).check();
        verify(client, times(1)).flush();
        verify(preExecutorMock, times(1)).execute();
        verify(disabledExecutorMock, never()).execute();
    }

    @Test
    public void testSmokeCheckExceptionPropagated() throws Exception {
        ReflectionTestUtils.setField(classToTest, "smokeTest", true);
        doThrow(new ServiceNotAvailableException("Not available")).when(healthCheck).check();
        assertThrows(ServiceNotAvailableException.class, () -> classToTest.run(null));
        verify(healthCheck, times(1)).check();
        verify(extractionComponent, never()).execute();
    }
}
