package uk.gov.hmcts.reform.dataextractor;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

import uk.gov.hmcts.reform.dataextractor.config.ExtractionData;
import uk.gov.hmcts.reform.dataextractor.config.Extractions;
import uk.gov.hmcts.reform.dataextractor.model.CaseDefinition;
import uk.gov.hmcts.reform.dataextractor.model.Output;
import uk.gov.hmcts.reform.dataextractor.service.Extractor;
import uk.gov.hmcts.reform.dataextractor.service.impl.BlobServiceImpl;
import uk.gov.hmcts.reform.dataextractor.service.impl.CaseDataServiceImpl;
import uk.gov.hmcts.reform.dataextractor.utils.BlobFileUtils;
import uk.gov.hmcts.reform.mi.micore.utils.DateTimeUtils;

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
class ExtractionComponentTest {

    private static final String CONTAINER_NAME = "testcontainer";
    private static final String CONTAINER_NAME2 = "testcontainer2";
    private static final String CASE_TYPE1 = "divorce";
    private static final String CASE_TYPE2 = "probate";

    private static final String PREFIX = "test";

    private static final ExtractionData TEST_EXTRACTOR_DATA = ExtractionData
        .builder()
        .container(CONTAINER_NAME)
        .caseType(CASE_TYPE1)
        .prefix(PREFIX)
        .type(Output.JSON_LINES)
        .build();
    private static final ExtractionData TEST_EXTRACTOR_DATA2 = ExtractionData
        .builder()
        .caseType(CASE_TYPE2)
        .type(Output.JSON_LINES)
        .prefix(PREFIX)
        .container(CONTAINER_NAME2)
        .build();

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
    void setUp() {
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        when(clock.instant()).thenReturn(Instant.parse("2020-02-01T18:35:24.00Z"));
        when(clock.getZone()).thenReturn(TimeZone.getTimeZone("UTC").toZoneId());
    }

    @Test
    void givenExtractorList_thenProcessAllCases() throws SQLException {

        List<ExtractionData> extractionData = Arrays.asList(TEST_EXTRACTOR_DATA, TEST_EXTRACTOR_DATA2);
        LocalDate updatedDate = LocalDate.now(clock);

        when(blobOutputFactory.provide(any(ExtractionData.class))).thenReturn(writer);
        when(queryExecutorFactory.provide(anyString())).thenReturn(queryExecutor);

        when(extractorFactory.provide(TEST_EXTRACTOR_DATA.getType())).thenReturn(extractor);
        when(extractorFactory.provide(TEST_EXTRACTOR_DATA2.getType())).thenReturn(extractor);

        when(extractions.getCaseTypes()).thenReturn(extractionData);
        when(queryExecutor.execute()).thenReturn(resultSet);
        when(resultSet.isBeforeFirst()).thenReturn(true);
        when(blobService.getContainerLastUpdated(CONTAINER_NAME)).thenReturn(updatedDate);
        when(blobService.getContainerLastUpdated(CONTAINER_NAME2)).thenReturn(updatedDate);

        when(blobService.validateBlob(anyString(), anyString(), any(Output.class))).thenReturn(true);
        when(caseDataService.getCaseDefinitions()).thenReturn(Arrays.asList(new CaseDefinition("", CASE_TYPE1), new CaseDefinition("", CASE_TYPE2)));

        classToTest.execute(true);

        verify(writer, times(2)).getOutputStream(BlobFileUtils.getFileName(TEST_EXTRACTOR_DATA, updatedDate));
        verify(blobService, times(1)).setLastUpdated(CONTAINER_NAME, updatedDate);
        verify(blobService, times(1)).setLastUpdated(CONTAINER_NAME2, updatedDate);
        verify(queryExecutor, times(2)).close();

    }

    @Test
    void givenNewCaseType_whenExtractData_thenProcessAllCases() throws SQLException {

        List<ExtractionData> extractionData = Arrays.asList(TEST_EXTRACTOR_DATA, TEST_EXTRACTOR_DATA2);
        LocalDate updatedDate = LocalDate.now(clock);

        when(blobOutputFactory.provide(any(ExtractionData.class))).thenReturn(writer);
        when(queryExecutorFactory.provide(anyString())).thenReturn(queryExecutor);
        when(caseDataService.getFirstEventDate(TEST_EXTRACTOR_DATA.getCaseType())).thenReturn(updatedDate);
        when(caseDataService.getFirstEventDate(TEST_EXTRACTOR_DATA2.getCaseType())).thenReturn(updatedDate);

        when(extractorFactory.provide(TEST_EXTRACTOR_DATA.getType())).thenReturn(extractor);
        when(extractorFactory.provide(TEST_EXTRACTOR_DATA2.getType())).thenReturn(extractor);

        when(extractions.getCaseTypes()).thenReturn(extractionData);
        when(queryExecutor.execute()).thenReturn(resultSet);
        when(resultSet.isBeforeFirst()).thenReturn(true);
        when(blobService.getContainerLastUpdated(CONTAINER_NAME)).thenReturn(null);

        when(caseDataService.getCaseDefinitions()).thenReturn(Arrays.asList(new CaseDefinition("", CASE_TYPE1), new CaseDefinition("", CASE_TYPE2)));
        classToTest.execute(true);

        verify(writer, times(2)).getOutputStream(BlobFileUtils.getFileName(TEST_EXTRACTOR_DATA, updatedDate));
    }

    @Test
    void givenErrorProcessingOneExtractor_thenProcessAll() throws NoSuchAlgorithmException {

        LocalDate fromDate = LocalDate.now(clock).minusMonths(6);

        List<ExtractionData> extractionData = Arrays.asList(TEST_EXTRACTOR_DATA, TEST_EXTRACTOR_DATA2);
        when(extractions.getCaseTypes()).thenReturn(extractionData);
        when(blobOutputFactory.provide(any(ExtractionData.class))).thenThrow(new RuntimeException("Any error"));
        when(queryExecutorFactory.provide(anyString())).thenReturn(queryExecutor);
        when(blobService.getContainerLastUpdated(CONTAINER_NAME)).thenReturn(fromDate);
        when(blobService.getContainerLastUpdated(CONTAINER_NAME2)).thenReturn(fromDate);

        when(caseDataService.getCaseDefinitions()).thenReturn(Arrays.asList(new CaseDefinition("", CASE_TYPE1), new CaseDefinition("", CASE_TYPE2)));

        classToTest.execute(true);

        verify(queryExecutorFactory, times(2)).provide(any());
        verify(queryExecutor, times(2)).close();

    }

    @Test
    void givenErrorOnLastUpdateProcessingOneExtractor_thenProcessAll() {
        List<ExtractionData> extractionData = Arrays.asList(TEST_EXTRACTOR_DATA, TEST_EXTRACTOR_DATA2);
        when(extractions.getCaseTypes()).thenReturn(extractionData);
        when(blobService.getContainerLastUpdated(CONTAINER_NAME)).thenThrow(new RuntimeException("Any error"));
        when(blobService.getContainerLastUpdated(CONTAINER_NAME2)).thenThrow(new RuntimeException("Any error"));

        when(caseDataService.getCaseDefinitions()).thenReturn(Arrays.asList(new CaseDefinition("", CASE_TYPE1), new CaseDefinition("", CASE_TYPE2)));

        classToTest.execute(true);

        verify(blobService, times(1)).getContainerLastUpdated(CONTAINER_NAME);
        verify(blobService, times(1)).getContainerLastUpdated(CONTAINER_NAME2);
        verify(queryExecutorFactory, never()).provide(anyString());
    }

    @Test
    void givenCorruptedFile_thenDeleteFile() throws SQLException {

        List<ExtractionData> extractionData = Arrays.asList(TEST_EXTRACTOR_DATA, TEST_EXTRACTOR_DATA2);
        LocalDate updatedDate = LocalDate.now(clock);
        final String blobName = BlobFileUtils.getFileName(TEST_EXTRACTOR_DATA, updatedDate);
        final String blobName2 = BlobFileUtils.getFileName(TEST_EXTRACTOR_DATA2, updatedDate);

        when(blobOutputFactory.provide(any(ExtractionData.class))).thenReturn(writer);
        when(queryExecutorFactory.provide(anyString())).thenReturn(queryExecutor);
        when(extractorFactory.provide(TEST_EXTRACTOR_DATA.getType())).thenReturn(extractor);
        when(extractorFactory.provide(TEST_EXTRACTOR_DATA2.getType())).thenReturn(extractor);

        when(extractions.getCaseTypes()).thenReturn(extractionData);
        when(queryExecutor.execute()).thenReturn(resultSet);
        when(resultSet.isBeforeFirst()).thenReturn(true);
        when(blobService.validateBlob(CONTAINER_NAME, blobName, Output.JSON_LINES))
            .thenReturn(false);
        when(blobService.validateBlob(CONTAINER_NAME2, blobName2, Output.JSON_LINES))
            .thenReturn(false);
        when(blobService.getContainerLastUpdated(CONTAINER_NAME)).thenReturn(updatedDate);
        when(blobService.getContainerLastUpdated(CONTAINER_NAME2)).thenReturn(updatedDate);

        when(caseDataService.getCaseDefinitions()).thenReturn(Arrays.asList(new CaseDefinition("", CASE_TYPE1), new CaseDefinition("", CASE_TYPE2)));
        classToTest.execute(true);

        verify(writer, times(2)).getOutputStream(blobName);
        verify(queryExecutor, times(2)).close();
        verify(blobService, times(1)).deleteBlob(CONTAINER_NAME, blobName);
        verify(blobService, times(1)).deleteBlob(CONTAINER_NAME2, blobName);

    }

    @Test
    void givenErrorOnOneExtractor_thenProcessAll() {
        LocalDate updatedDate = LocalDate.now(clock);
        List<ExtractionData> extractionData = Arrays.asList(TEST_EXTRACTOR_DATA, TEST_EXTRACTOR_DATA2);
        when(extractions.getCaseTypes()).thenReturn(extractionData);
        when(blobService.getContainerLastUpdated(CONTAINER_NAME)).thenReturn(updatedDate);
        when(blobService.getContainerLastUpdated(CONTAINER_NAME2)).thenReturn(updatedDate);
        when(extractorFactory.provide(Output.JSON_LINES)).thenReturn(extractor);
        when(caseDataService.getCaseDefinitions()).thenReturn(Arrays.asList(new CaseDefinition("", CASE_TYPE1), new CaseDefinition("", CASE_TYPE2)));
        when(caseDataService.calculateExtractionWindow(any(), any(), any(), anyBoolean())).thenReturn(5);
        when(queryExecutorFactory.provide(any())).thenReturn(queryExecutor);
        when(blobOutputFactory.provide(any())).thenThrow(new RuntimeException());

        classToTest.execute(true);

        verify(blobService, times(1)).getContainerLastUpdated(CONTAINER_NAME);
        verify(blobService, times(1)).getContainerLastUpdated(CONTAINER_NAME2);
        verify(queryExecutor, times(2)).close();
    }

    @Test
    void givenNotInitialised_whenExtractData_thenProcessOthers() throws SQLException {

        List<ExtractionData> extractionData = Arrays.asList(TEST_EXTRACTOR_DATA, TEST_EXTRACTOR_DATA2);
        LocalDate updatedDate = LocalDate.now(clock);

        when(blobOutputFactory.provide(any(ExtractionData.class))).thenReturn(writer);
        when(queryExecutorFactory.provide(anyString())).thenReturn(queryExecutor);

        when(extractorFactory.provide(TEST_EXTRACTOR_DATA.getType())).thenReturn(extractor);
        when(extractorFactory.provide(TEST_EXTRACTOR_DATA2.getType())).thenReturn(extractor);

        when(extractions.getCaseTypes()).thenReturn(extractionData);
        when(queryExecutor.execute()).thenReturn(resultSet);
        when(resultSet.isBeforeFirst()).thenReturn(true);
        when(blobService.getContainerLastUpdated(CONTAINER_NAME)).thenReturn(updatedDate);

        when(caseDataService.getCaseDefinitions()).thenReturn(Arrays.asList(new CaseDefinition("", CASE_TYPE1), new CaseDefinition("", CASE_TYPE2)));

        classToTest.execute(false);

        verify(writer, times(1)).getOutputStream(BlobFileUtils.getFileName(TEST_EXTRACTOR_DATA, updatedDate));
    }

    @Test
    void givenToDate_whenExtract_thenProcessAllCases() throws SQLException {
        ReflectionTestUtils.setField(classToTest, "limitDate", "20200303");

        List<ExtractionData> extractionData = Arrays.asList(TEST_EXTRACTOR_DATA);
        LocalDate updatedDate = LocalDate.now(clock);

        when(blobOutputFactory.provide(any(ExtractionData.class))).thenReturn(writer);
        when(queryExecutorFactory.provide(anyString())).thenReturn(queryExecutor);

        when(extractorFactory.provide(TEST_EXTRACTOR_DATA.getType())).thenReturn(extractor);

        when(extractions.getCaseTypes()).thenReturn(extractionData);
        when(queryExecutor.execute()).thenReturn(resultSet);
        when(resultSet.isBeforeFirst()).thenReturn(true);
        when(blobService.getContainerLastUpdated(CONTAINER_NAME)).thenReturn(updatedDate);
        when(caseDataService.calculateExtractionWindow(any(), any(), any(), anyBoolean())).thenReturn(100);

        when(blobService.validateBlob(anyString(), anyString(), any(Output.class))).thenReturn(true);
        when(caseDataService.getCaseDefinitions()).thenReturn(Arrays.asList(new CaseDefinition("", CASE_TYPE1)));

        classToTest.execute(true);
        LocalDate toDate = DateTimeUtils.stringToLocalDate("20200303");
        verify(writer, times(1)).getOutputStream(BlobFileUtils.getFileName(TEST_EXTRACTOR_DATA, toDate));
        verify(blobService, times(1)).setLastUpdated(CONTAINER_NAME, toDate);
        verify(queryExecutor, times(1)).close();
    }
}
