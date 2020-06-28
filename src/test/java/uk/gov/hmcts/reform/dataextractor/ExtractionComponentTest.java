package uk.gov.hmcts.reform.dataextractor;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

import uk.gov.hmcts.reform.dataextractor.config.ExtractionData;
import uk.gov.hmcts.reform.dataextractor.config.Extractions;
import uk.gov.hmcts.reform.dataextractor.model.CaseDefinition;
import uk.gov.hmcts.reform.dataextractor.model.Output;
import uk.gov.hmcts.reform.dataextractor.service.Extractor;
import uk.gov.hmcts.reform.dataextractor.service.impl.BlobServiceImpl;
import uk.gov.hmcts.reform.dataextractor.service.impl.CaseDataServiceImpl;
import uk.gov.hmcts.reform.dataextractor.utils.BlobFileUtils;

import java.security.NoSuchAlgorithmException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Clock;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;
import java.util.TimeZone;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class ExtractionComponentTest {

    private static final String CONTAINER_NAME = "testcontainer";
    private static final String CONTAINER_NAME2 = "testcontainer2";
    private static final String CASE_TYPE1 = "divorce";
    private static final String CASE_TYPE2 = "probate";

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

    @Mock
    private Clock clock;

    @BeforeEach
    public void setUp() {
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        when(clock.instant()).thenReturn(Instant.parse("2020-02-01T18:35:24.00Z"));
        when(clock.getZone()).thenReturn(TimeZone.getTimeZone("UTC").toZoneId());
    }

    @Test
    public void givenExtractorList_thenProcessAllCases() throws SQLException {
        ExtractionData testExtractorData = ExtractionData
            .builder()
            .container(CONTAINER_NAME)
            .prefix(PREFIX)
            .caseType(CASE_TYPE1)
            .type(Output.JSON_LINES)
            .build();

        ExtractionData testExtractorData2 = ExtractionData
            .builder()
            .caseType(CASE_TYPE2)
            .container(CONTAINER_NAME)
            .prefix(PREFIX)
            .type(Output.JSON_LINES)
            .build();

        List<ExtractionData> extractionData = Arrays.asList(testExtractorData, testExtractorData2);
        LocalDate updatedDate = LocalDate.now(clock);

        when(blobOutputFactory.provide(any(ExtractionData.class))).thenReturn(writer);
        when(queryExecutorFactory.provide(anyString())).thenReturn(queryExecutor);

        when(extractorFactory.provide(testExtractorData.getType())).thenReturn(extractor);
        when(extractorFactory.provide(testExtractorData2.getType())).thenReturn(extractor);

        when(extractions.getCaseTypes()).thenReturn(extractionData);
        when(queryExecutor.execute()).thenReturn(resultSet);
        when(resultSet.isBeforeFirst()).thenReturn(true);
        when(blobService.getContainerLastUpdated(CONTAINER_NAME, true)).thenReturn(updatedDate);
        when(blobService.validateBlob(anyString(), anyString(), any(Output.class))).thenReturn(true);
        when(caseDataService.getCaseDefinitions()).thenReturn(Arrays.asList(new CaseDefinition("", CASE_TYPE1), new CaseDefinition("", CASE_TYPE2)));

        classToTest.execute(true);

        verify(writer, times(2)).outputStream(BlobFileUtils.getFileName(testExtractorData, updatedDate));
        verify(blobService, times(2)).setLastUpdated(CONTAINER_NAME, updatedDate);
        verify(queryExecutor, times(2)).close();

    }

    @Test
    public void givenNewCaseType_whenExtractData_thenProcessAllCases() throws SQLException {
        ExtractionData testExtractorData = ExtractionData
            .builder()
            .container(CONTAINER_NAME)
            .prefix(PREFIX)
            .caseType(CASE_TYPE1)
            .type(Output.JSON_LINES)
            .build();

        ExtractionData testExtractorData2 = ExtractionData
            .builder()
            .caseType(CASE_TYPE2)
            .container(CONTAINER_NAME)
            .prefix(PREFIX)
            .type(Output.JSON_LINES)
            .build();


        List<ExtractionData> extractionData = Arrays.asList(testExtractorData, testExtractorData2);
        LocalDate updatedDate = LocalDate.now(clock);

        when(blobOutputFactory.provide(any(ExtractionData.class))).thenReturn(writer);
        when(queryExecutorFactory.provide(anyString())).thenReturn(queryExecutor);
        when(caseDataService.getFirstEventDate(testExtractorData.getCaseType())).thenReturn(updatedDate);
        when(caseDataService.getFirstEventDate(testExtractorData2.getCaseType())).thenReturn(updatedDate);

        when(extractorFactory.provide(testExtractorData.getType())).thenReturn(extractor);
        when(extractorFactory.provide(testExtractorData2.getType())).thenReturn(extractor);

        when(extractions.getCaseTypes()).thenReturn(extractionData);
        when(queryExecutor.execute()).thenReturn(resultSet);
        when(resultSet.isBeforeFirst()).thenReturn(true);
        when(blobService.getContainerLastUpdated(CONTAINER_NAME, true)).thenReturn(null);

        when(caseDataService.getCaseDefinitions()).thenReturn(Arrays.asList(new CaseDefinition("", CASE_TYPE1), new CaseDefinition("", CASE_TYPE2)));
        classToTest.execute(true);

        verify(writer, times(2)).outputStream(BlobFileUtils.getFileName(testExtractorData, updatedDate));
    }

    @Test
    public void givenErrorProcessingOneExtractor_thenProcessAll() throws NoSuchAlgorithmException {
        ExtractionData testExtractorData = ExtractionData
            .builder()
            .caseType(CASE_TYPE1)
            .container(CONTAINER_NAME)
            .build();
        LocalDate fromDate = LocalDate.now(clock).minusMonths(6);

        ExtractionData testExtractorData2 = ExtractionData
            .builder()
            .caseType(CASE_TYPE2)
            .container(CONTAINER_NAME)
            .prefix(PREFIX)
            .type(Output.JSON_LINES)
            .build();

        List<ExtractionData> extractionData = Arrays.asList(testExtractorData, testExtractorData2);
        when(extractions.getCaseTypes()).thenReturn(extractionData);
        when(blobOutputFactory.provide(any(ExtractionData.class))).thenThrow(new RuntimeException("Any error"));
        when(queryExecutorFactory.provide(anyString())).thenReturn(queryExecutor);
        when(blobService.getContainerLastUpdated(CONTAINER_NAME, true)).thenReturn(fromDate);
        when(caseDataService.getCaseDefinitions()).thenReturn(Arrays.asList(new CaseDefinition("", CASE_TYPE1), new CaseDefinition("", CASE_TYPE2)));

        classToTest.execute(true);

        verify(queryExecutorFactory, times(2)).provide(any());
        verify(queryExecutor, times(2)).close();

    }

    @Test
    public void givenErrorOnLastUpdateProcessingOneExtractor_thenProcessAll() {
        ExtractionData testExtractorData = ExtractionData
            .builder()
            .container(CONTAINER_NAME)
            .caseType(CASE_TYPE1)
            .build();
        ExtractionData testExtractorData2 = ExtractionData
            .builder()
            .caseType(CASE_TYPE2)
            .container(CONTAINER_NAME2)
            .build();
        List<ExtractionData> extractionData = Arrays.asList(testExtractorData, testExtractorData2);
        when(extractions.getCaseTypes()).thenReturn(extractionData);
        when(blobService.getContainerLastUpdated(CONTAINER_NAME, true)).thenThrow(new RuntimeException("Any error"));
        when(blobService.getContainerLastUpdated(CONTAINER_NAME2, true)).thenThrow(new RuntimeException("Any error"));

        when(caseDataService.getCaseDefinitions()).thenReturn(Arrays.asList(new CaseDefinition("", CASE_TYPE1), new CaseDefinition("", CASE_TYPE2)));

        classToTest.execute(true);

        verify(blobService, times(1)).getContainerLastUpdated(CONTAINER_NAME, true);
        verify(blobService, times(1)).getContainerLastUpdated(CONTAINER_NAME2, true);
        verify(queryExecutorFactory, never()).provide(anyString());
    }

    @Test
    public void givenCorruptedFile_thenDeleteFile() throws SQLException {
        ExtractionData testExtractorData = ExtractionData
            .builder()
            .container(CONTAINER_NAME)
            .prefix(PREFIX)
            .caseType(CASE_TYPE1)
            .type(Output.JSON_LINES)
            .build();
        ExtractionData testExtractorData2 = ExtractionData
            .builder()
            .caseType(CASE_TYPE2)
            .prefix(PREFIX)
            .container(CONTAINER_NAME)
            .type(Output.JSON_LINES)
            .build();
        List<ExtractionData> extractionData = Arrays.asList(testExtractorData, testExtractorData2);
        LocalDate updatedDate = LocalDate.now(clock);
        final String blobName = BlobFileUtils.getFileName(testExtractorData, updatedDate);
        final String blobName2 = BlobFileUtils.getFileName(testExtractorData2, updatedDate);

        when(blobOutputFactory.provide(any(ExtractionData.class))).thenReturn(writer);
        when(queryExecutorFactory.provide(anyString())).thenReturn(queryExecutor);
        when(extractorFactory.provide(testExtractorData.getType())).thenReturn(extractor);
        when(extractorFactory.provide(testExtractorData2.getType())).thenReturn(extractor);

        when(extractions.getCaseTypes()).thenReturn(extractionData);
        when(queryExecutor.execute()).thenReturn(resultSet);
        when(resultSet.isBeforeFirst()).thenReturn(true);
        when(blobService.validateBlob(CONTAINER_NAME, blobName, Output.JSON_LINES))
            .thenReturn(false);
        when(blobService.validateBlob(CONTAINER_NAME, blobName2, Output.JSON_LINES))
            .thenReturn(false);
        when(blobService.getContainerLastUpdated(CONTAINER_NAME, true)).thenReturn(updatedDate);
        when(caseDataService.getCaseDefinitions()).thenReturn(Arrays.asList(new CaseDefinition("", CASE_TYPE1), new CaseDefinition("", CASE_TYPE2)));
        classToTest.execute(true);

        verify(writer, times(2)).outputStream(blobName);
        verify(queryExecutor, times(2)).close();
        verify(blobService, times(2)).deleteBlob(CONTAINER_NAME, blobName);
    }

    @Test
    public void givenErrorOnOneExtractor_thenProcessAll() {
        ExtractionData testExtractorData = ExtractionData
            .builder()
            .container(CONTAINER_NAME)
            .caseType(CASE_TYPE1)
            .type(Output.JSON)
            .build();
        ExtractionData testExtractorData2 = ExtractionData
            .builder()
            .caseType(CASE_TYPE2)
            .type(Output.JSON)
            .container(CONTAINER_NAME2)
            .build();
        LocalDate updatedDate = LocalDate.now(clock);
        List<ExtractionData> extractionData = Arrays.asList(testExtractorData, testExtractorData2);
        when(extractions.getCaseTypes()).thenReturn(extractionData);
        when(blobService.getContainerLastUpdated(CONTAINER_NAME, true)).thenReturn(updatedDate);
        when(blobService.getContainerLastUpdated(CONTAINER_NAME2, true)).thenReturn(updatedDate);
        when(extractorFactory.provide(Output.JSON)).thenReturn(extractor);
        when(caseDataService.getCaseDefinitions()).thenReturn(Arrays.asList(new CaseDefinition("", CASE_TYPE1), new CaseDefinition("", CASE_TYPE2)));
        when(caseDataService.calculateExtractionWindow(any(), any(), any(), anyBoolean())).thenReturn(5);
        when(queryExecutorFactory.provide(any())).thenReturn(queryExecutor);
        when(blobOutputFactory.provide(any())).thenThrow(new RuntimeException());
        classToTest.execute(true);

        verify(blobService, times(1)).getContainerLastUpdated(CONTAINER_NAME, true);
        verify(blobService, times(1)).getContainerLastUpdated(CONTAINER_NAME2, true);
        verify(queryExecutor, times(2)).close();
    }
}
