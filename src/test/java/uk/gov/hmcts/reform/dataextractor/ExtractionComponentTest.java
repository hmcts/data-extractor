package uk.gov.hmcts.reform.dataextractor;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

import uk.gov.hmcts.reform.dataextractor.config.ExtractionData;
import uk.gov.hmcts.reform.dataextractor.config.Extractions;
import uk.gov.hmcts.reform.dataextractor.model.Output;
import uk.gov.hmcts.reform.dataextractor.service.Extractor;
import uk.gov.hmcts.reform.dataextractor.service.impl.BlobServiceImpl;
import uk.gov.hmcts.reform.dataextractor.service.impl.CaseDataServiceImpl;
import uk.gov.hmcts.reform.dataextractor.utils.BlobFileUtils;

import java.security.NoSuchAlgorithmException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class ExtractionComponentTest {

    private static final String CONTAINER_NAME = "testContainer";
    private static final String PREFIX = "test";

    @InjectMocks
    private ExtractionComponent classToTest;

    @Spy
    private Extractions extractions;

    @Mock
    private Factory<ExtractionData, BlobOutputWriter> blobOutputFactory;

    @Mock
    private Factory<String,  QueryExecutor> queryExecutorFactory;

    @Mock
    private Factory<Output, Extractor> extractorFactory;

    @Mock
    private BlobOutputWriter writer;

    @Mock
    private QueryExecutor queryExecutor;

    @Mock
    private BlobServiceImpl blobService;

    @Mock
    private Extractor extractor;

    @Mock
    private ResultSet resultSet;

    @Mock
    private CaseDataServiceImpl caseDataService;

    @Test
    public void givenExtractorList_thenProcessAllCases() throws SQLException {
        ExtractionData testExtractorData = ExtractionData
            .builder()
            .container(CONTAINER_NAME)
            .prefix(PREFIX)
            .type(Output.JSON_LINES)
            .build();


        List<ExtractionData> extractionData = Arrays.asList(testExtractorData, testExtractorData);
        LocalDate updatedDate = LocalDate.now();
        String query = QueryBuilder
            .builder()
            .fromDate(updatedDate)
            .toDate(updatedDate)
            .extractionData(testExtractorData)
            .build()
            .getQuery();
        when(blobOutputFactory.provide(any(ExtractionData.class))).thenReturn(writer);
        when(queryExecutorFactory.provide(query)).thenReturn(queryExecutor);

        when(extractorFactory.provide(testExtractorData.getType())).thenReturn(extractor);
        when(extractions.getCaseTypes()).thenReturn(extractionData);
        when(queryExecutor.execute()).thenReturn(resultSet);
        when(resultSet.isBeforeFirst()).thenReturn(true);
        when(blobService.getContainerLastUpdated(CONTAINER_NAME)).thenReturn(updatedDate);
        classToTest.execute();

        verify(writer, times(2)).outputStream(BlobFileUtils.getFileName(testExtractorData, updatedDate));
    }

    @Test
    public void givenNewCaseType_whenExtractData_thenProcessAllCases() throws SQLException {
        ExtractionData testExtractorData = ExtractionData
            .builder()
            .container(CONTAINER_NAME)
            .prefix(PREFIX)
            .type(Output.JSON_LINES)
            .build();


        List<ExtractionData> extractionData = Arrays.asList(testExtractorData, testExtractorData);
        LocalDate updatedDate = LocalDate.now();
        String query = QueryBuilder
            .builder()
            .fromDate(updatedDate)
            .toDate(updatedDate)
            .extractionData(testExtractorData)
            .build()
            .getQuery();
        when(blobOutputFactory.provide(any(ExtractionData.class))).thenReturn(writer);
        when(queryExecutorFactory.provide(query)).thenReturn(queryExecutor);
        when(caseDataService.getFirstEventDate(testExtractorData.getCaseType())).thenReturn(updatedDate);
        when(extractorFactory.provide(testExtractorData.getType())).thenReturn(extractor);
        when(extractions.getCaseTypes()).thenReturn(extractionData);
        when(queryExecutor.execute()).thenReturn(resultSet);
        when(resultSet.isBeforeFirst()).thenReturn(true);
        when(blobService.getContainerLastUpdated(CONTAINER_NAME)).thenReturn(null);
        classToTest.execute();

        verify(writer, times(2)).outputStream(BlobFileUtils.getFileName(testExtractorData, updatedDate));
        verify(writer, times(2)).close();

    }

    @Test
    public void givenErrorProcessingOneExtractor_thenProcessAll() throws NoSuchAlgorithmException {
        ExtractionData testExtractorData = ExtractionData
            .builder()
            .container(CONTAINER_NAME)
            .build();
        LocalDate updatedDate = LocalDate.now();
        final String query = QueryBuilder
            .builder()
            .fromDate(updatedDate)
            .toDate(updatedDate)
            .extractionData(testExtractorData)
            .build()
            .getQuery();
        List<ExtractionData> extractionData = Arrays.asList(testExtractorData, testExtractorData);
        when(extractions.getCaseTypes()).thenReturn(extractionData);
        when(blobOutputFactory.provide(any(ExtractionData.class))).thenThrow(new RuntimeException("Any error"));
        when(queryExecutorFactory.provide(query)).thenReturn(queryExecutor);
        when(blobService.getContainerLastUpdated(CONTAINER_NAME)).thenReturn(updatedDate);

        classToTest.execute();

        verify(queryExecutorFactory, times(2)).provide(query);
    }

    @Test
    public void givenErrorOnLastUpdateProcessingOneExtractor_thenProcessAll() {
        ExtractionData testExtractorData = ExtractionData
            .builder()
            .container(CONTAINER_NAME)
            .build();
        List<ExtractionData> extractionData = Arrays.asList(testExtractorData, testExtractorData);
        when(extractions.getCaseTypes()).thenReturn(extractionData);
        when(blobService.getContainerLastUpdated(CONTAINER_NAME)).thenThrow(new RuntimeException("Any error"));

        classToTest.execute();

        verify(blobService, times(2)).getContainerLastUpdated(CONTAINER_NAME);
        verify(queryExecutorFactory, never()).provide(anyString());
    }
}
