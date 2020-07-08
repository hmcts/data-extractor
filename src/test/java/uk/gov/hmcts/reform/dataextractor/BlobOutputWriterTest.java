package uk.gov.hmcts.reform.dataextractor;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import uk.gov.hmcts.reform.dataextractor.model.Output;
import uk.gov.hmcts.reform.dataextractor.service.OutputStreamProvider;

import java.io.IOException;
import java.io.OutputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class BlobOutputWriterTest {

    private static final String CONTAINER_NAME = "testContainer";
    private static final String FILE_PREFIX_NAME = "filePrefix";
    private static final Output OUTPUT_TYPE = Output.JSON;

    private BlobOutputWriter classToTest;

    @Mock
    OutputStreamProvider outputStreamProviderMock;

    @Mock
    OutputStream outputStreamMock;

    @BeforeEach
    public void setup() {
        classToTest = new BlobOutputWriter(CONTAINER_NAME, FILE_PREFIX_NAME, OUTPUT_TYPE, outputStreamProviderMock);
    }

    @Test
    public void testStreamClose() throws IOException {
        when(outputStreamProviderMock.getOutputStream(eq(CONTAINER_NAME), anyString(), eq(OUTPUT_TYPE))).thenReturn(outputStreamMock);
        assertEquals(outputStreamMock, classToTest.outputStream());
        classToTest.close();
        verify(outputStreamMock,times(1)).close();
    }

    @Test
    public void whenStreamCloseError_thenErrorIsNotPropagated() throws IOException {
        when(outputStreamProviderMock.getOutputStream(eq(CONTAINER_NAME), anyString(), eq(OUTPUT_TYPE))).thenReturn(outputStreamMock);
        doThrow(new RuntimeException("Error")).when(outputStreamMock).close();
        classToTest.outputStream();
        classToTest.close();
        verify(outputStreamMock,times(1)).close();
    }
}
