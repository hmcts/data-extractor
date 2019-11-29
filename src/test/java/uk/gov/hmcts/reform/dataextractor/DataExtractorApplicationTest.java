package uk.gov.hmcts.reform.dataextractor;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.hmcts.reform.dataextractor.config.ExtractionData;
import uk.gov.hmcts.reform.dataextractor.config.Extractions;

import java.util.Arrays;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class DataExtractorApplicationTest {

    private static final String CONTAINER_NAME = "testContainer";
    private static final String QUERY = "testQuery";
    private static final String PREFIX = "test";

    @InjectMocks
    private DataExtractorApplication classToTest;

    @Spy
    private Extractions extractions;

    @Mock
    private Factory<ExtractionData, BlobOutputWriter> blobOutputFactory;

    @Mock
    private Factory<String,  QueryExecutor> queryExecutorFactory;

    @Mock
    private Factory<DataExtractorApplication.Output,  Extractor> extractorFactory;

    @Mock
    private BlobOutputWriter writer;

    @Mock
    private QueryExecutor queryExecutor;

    @Test
    public void givenExtractorList_thenProcessAllCases() {
        ExtractionData testExtractorData = ExtractionData
            .builder()
            .container(CONTAINER_NAME)
            .prefix(PREFIX)
            .query(QUERY)
            .type(DataExtractorApplication.Output.JSON_LINES)
            .build();
        List<ExtractionData> extractionData = Arrays.asList(testExtractorData, testExtractorData);

        when(blobOutputFactory.provide(any(ExtractionData.class))).thenReturn(writer);
        when(queryExecutorFactory.provide(QUERY)).thenReturn(queryExecutor);
        when(extractorFactory.provide(testExtractorData.getType())).thenReturn(mock(Extractor.class));
        when(extractions.getCaseTypes()).thenReturn(extractionData);

        classToTest.run(null);

        verify(writer, times(2)).outputStream();
    }

    @Test
    public void givenErrorProcessingOneExtractor_thenProcessAll() {
        ExtractionData testExtractorData = ExtractionData
            .builder()
            .query(QUERY)
            .build();
        List<ExtractionData> extractionData = Arrays.asList(testExtractorData, testExtractorData);
        when(extractions.getCaseTypes()).thenReturn(extractionData);
        when(blobOutputFactory.provide(any(ExtractionData.class))).thenThrow(new RuntimeException("Any error"));
        when(queryExecutorFactory.provide(QUERY)).thenReturn(queryExecutor);

        classToTest.run(null);

        verify(queryExecutorFactory, times(2)).provide(testExtractorData.getQuery());
    }
}
