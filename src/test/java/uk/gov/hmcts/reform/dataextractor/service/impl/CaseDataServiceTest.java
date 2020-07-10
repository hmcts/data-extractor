package uk.gov.hmcts.reform.dataextractor.service.impl;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

import uk.gov.hmcts.reform.dataextractor.Factory;
import uk.gov.hmcts.reform.dataextractor.QueryExecutor;
import uk.gov.hmcts.reform.dataextractor.config.ExtractionFilters;
import uk.gov.hmcts.reform.dataextractor.exception.ExtractorException;
import uk.gov.hmcts.reform.dataextractor.model.CaseDefinition;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CaseDataServiceTest {

    private static final String JURISDICTION_COLUMN = "jurisdiction";
    private static final String CASE_TYPE_COLUMN = "case_type_id";
    public static final String COUNT_COLUMN = "count";
    private static final String CASE_TYPE = "TEST";

    @Mock
    private Factory<String, QueryExecutor> queryExecutorFactory;

    @Mock
    private QueryExecutor queryExecutor;

    @Mock
    private ResultSet resultSet;

    @Mock
    private ExtractionFilters filters;

    @InjectMocks
    private CaseDataServiceImpl classToTest;

    @BeforeEach
    void setUp() {
        ReflectionTestUtils.setField(classToTest, "maxRowPerBatch", 100);
    }

    @Test
    void testNoFirstEvent_thenRaiseError() throws SQLException {
        when(queryExecutorFactory.provide(anyString())).thenReturn(queryExecutor);
        when(queryExecutor.execute()).thenReturn(resultSet);

        when(resultSet.next()).thenReturn(false);

        assertThrows(ExtractorException.class, () -> classToTest.getFirstEventDate(""),
            "Expected ExtractorException");

        verify(queryExecutor, times(1)).close();
    }


    @Test
    void givenSqlException_thenRaiseExtractionException() throws SQLException {
        when(queryExecutorFactory.provide(anyString())).thenReturn(queryExecutor);
        when(queryExecutor.execute()).thenReturn(resultSet);

        when(resultSet.next()).thenThrow(new SQLException());

        assertThrows(ExtractorException.class, () -> classToTest.getFirstEventDate(""),
            "Expected ExtractorException");

        verify(queryExecutor, times(1)).close();
    }

    @Test
    void testCheckConnection() throws SQLException {
        when(queryExecutorFactory.provide(anyString())).thenReturn(queryExecutor);
        when(queryExecutor.execute()).thenReturn(resultSet);
        classToTest.checkConnection();

        verify(queryExecutor, times(1)).close();
        verify(resultSet, times(1)).next();
    }

    @Test
    void givenSqlException_whenCheckConnection_thenRaiseExtractionException() throws SQLException {
        when(queryExecutorFactory.provide(anyString())).thenReturn(queryExecutor);
        when(queryExecutor.execute()).thenReturn(resultSet);

        when(resultSet.next()).thenThrow(new SQLException());

        assertThrows(ExtractorException.class, () -> classToTest.checkConnection(),
            "Expected ExtractorException");

        verify(queryExecutor, times(1)).close();
    }

    @Test
    void givenOutFilter_whenGetCaseDefinition_thenReturnCaseDefinitions() throws SQLException {
        String expectedQuery = "select distinct jurisdiction, case_type_id from case_data where jurisdiction not in ('exclusion1', 'exclusion2')";

        when(filters.getOut()).thenReturn(Arrays.asList("'exclusion1', 'exclusion2'"));
        when(queryExecutorFactory.provide(expectedQuery)).thenReturn(queryExecutor);
        when(queryExecutor.execute()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true, false);
        when(resultSet.getString(JURISDICTION_COLUMN)).thenReturn("jurisdiction");
        when(resultSet.getString(CASE_TYPE_COLUMN)).thenReturn("case_type_id");

        List<CaseDefinition> expected = Arrays.asList(new CaseDefinition("jurisdiction", "case_type_id"));

        List<CaseDefinition> definitions = classToTest.getCaseDefinitions();

        assertEquals(expected, definitions, "Expected exclusion data");
    }

    @Test
    void givenInFilter_whenGetCaseDefinition_thenReturnCaseDefinitions() throws SQLException {
        String expectedQuery = "select distinct jurisdiction, case_type_id from case_data where jurisdiction in ('inclusion1', 'inclusion2')";

        when(filters.getIn()).thenReturn(Arrays.asList("'inclusion1', 'inclusion2'"));
        when(queryExecutorFactory.provide(expectedQuery)).thenReturn(queryExecutor);
        when(queryExecutor.execute()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true, false);
        when(resultSet.getString(JURISDICTION_COLUMN)).thenReturn("jurisdiction");
        when(resultSet.getString(CASE_TYPE_COLUMN)).thenReturn("case_type_id");

        List<CaseDefinition> expected = Arrays.asList(new CaseDefinition("jurisdiction", "case_type_id"));

        List<CaseDefinition> definitions = classToTest.getCaseDefinitions();

        assertEquals(expected, definitions, "Expected exclusion data");
        verify(queryExecutor, times(1)).close();
    }

    @Test
    void givenError_whenGetCaseDefinition_thenRaiseError() throws SQLException {
        String expectedQuery = "select distinct jurisdiction, case_type_id from case_data where jurisdiction in ('inclusion1', 'inclusion2')";

        when(filters.getIn()).thenReturn(Arrays.asList("'inclusion1', 'inclusion2'"));
        when(queryExecutorFactory.provide(expectedQuery)).thenReturn(queryExecutor);
        when(queryExecutor.execute()).thenReturn(resultSet);
        when(resultSet.next()).thenThrow(new SQLException());

        assertThrows(ExtractorException.class, () -> classToTest.getCaseDefinitions(),
            "Expected ExtractorException");

        verify(queryExecutor, times(1)).close();
    }

    @ParameterizedTest
    @CsvSource({"'2000-01-01', '2000-01-01', 1000, 7", //one day data
        "'2000-01-01', '2001-01-31', 1000, 40", // normal flow
        "'2000-01-01', '2001-01-31', 0, 7", // normal flow
        "'2000-01-01', '2000-01-31', 10, 30"}) // Less data than window
    void testCalculateExtractionWindow(String initDate, String endDate, long totalCount, int expectedWindow) throws SQLException {

        final String countQuery = "select count(*) \n"
            + "FROM case_event \n"
            + "WHERE case_type_id = 'TEST';";

        when(queryExecutorFactory.provide(countQuery)).thenReturn(queryExecutor);
        when(queryExecutor.execute()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getLong(COUNT_COLUMN)).thenReturn(totalCount);

        assertEquals(expectedWindow, classToTest.calculateExtractionWindow(CASE_TYPE, LocalDate.parse(initDate), LocalDate.parse(endDate), true));

        verify(queryExecutor, times(1)).close();
    }

    @Test
    void givenError_whenCalculateExtractionWindow_thenRaiseError() throws SQLException {
        final String countQuery = "select count(*) \n"
            + "FROM case_event \n"
            + "WHERE case_type_id = 'TEST';";

        when(queryExecutorFactory.provide(countQuery)).thenReturn(queryExecutor);
        when(queryExecutor.execute()).thenReturn(resultSet);
        when(resultSet.next()).thenThrow(new SQLException());
        LocalDate executionTime = LocalDate.now();
        assertThrows(ExtractorException.class, () -> classToTest.calculateExtractionWindow(CASE_TYPE, executionTime, executionTime, true),
            "Expected ExtractorException");

        verify(queryExecutor, times(1)).close();
    }

    @Test
    void testCalculateRowCount() throws SQLException {

        final String countQuery = "select count(*) \n"
            + "FROM case_event \n"
            + "WHERE case_type_id = 'TEST';";

        when(queryExecutorFactory.provide(countQuery)).thenReturn(queryExecutor);
        when(queryExecutor.execute()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(false);

        assertEquals(0, classToTest.getCaseTypeRows(CASE_TYPE));

        verify(queryExecutor, times(1)).close();
    }

    @Test
    void testCalculateRowCountException() throws SQLException {

        when(queryExecutorFactory.provide(any())).thenReturn(queryExecutor);
        when(queryExecutor.execute()).thenReturn(resultSet);
        when(resultSet.next()).thenThrow(new SQLException());
        assertThrows(ExtractorException.class,() -> classToTest.getCaseTypeRows(CASE_TYPE));

        verify(queryExecutor, times(1)).close();
    }

}
