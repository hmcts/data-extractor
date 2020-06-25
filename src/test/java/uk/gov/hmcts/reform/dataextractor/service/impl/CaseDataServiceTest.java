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
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class CaseDataServiceTest {

    private static final String JURISDICTION_COLUMN = "jurisdiction";
    private static final String CASE_TYPE_COLUMN = "case_type_id";
    public static final String COUNT_COLUMN = "count";
    public static final String LAST_DATE_COLUMN = "last_date";
    public static final String FIRST_DATE_COLUMN = "first_date";

    @Mock
    private Factory<String, QueryExecutor> queryExecutorFactory;

    @Mock
    private QueryExecutor queryExecutor;

    @Mock
    private QueryExecutor additionalQueryExecutor;

    @Mock
    private ResultSet additionalResultSet;

    @Mock
    private ResultSet resultSet;

    @Mock
    private ExtractionFilters filters;

    @InjectMocks
    private CaseDataServiceImpl classToTest;
    public static final String CASE_TYPE = "TEST";

    @BeforeEach
    public void setUp() {
        ReflectionTestUtils.setField(classToTest, "maxRowPerBatch", 100);
    }

    @Test
    public void givenSqlException_thenRaiseExtractionException() throws SQLException {
        when(queryExecutorFactory.provide(anyString())).thenReturn(queryExecutor);
        when(queryExecutor.execute()).thenReturn(resultSet);

        when(resultSet.next()).thenThrow(new SQLException());

        assertThrows(ExtractorException.class, () -> classToTest.getFirstEventDate(""),
            "Expected ExtractorException");

        verify(queryExecutor, times(1)).close();
    }

    @Test
    public void testCheckConnection() throws SQLException {
        when(queryExecutorFactory.provide(anyString())).thenReturn(queryExecutor);
        when(queryExecutor.execute()).thenReturn(resultSet);
        classToTest.checkConnection();

        verify(queryExecutor, times(1)).close();
        verify(resultSet, times(1)).next();
    }

    @Test
    public void givenSqlException_whenCheckConnection_thenRaiseExtractionException() throws SQLException {
        when(queryExecutorFactory.provide(anyString())).thenReturn(queryExecutor);
        when(queryExecutor.execute()).thenReturn(resultSet);

        when(resultSet.next()).thenThrow(new SQLException());

        assertThrows(ExtractorException.class, () -> classToTest.checkConnection(),
            "Expected ExtractorException");

        verify(queryExecutor, times(1)).close();
    }

    @Test
    public void givenOutFilter_whenGetCaseDefinition_thenReturnCaseDefinitions() throws SQLException {
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
    public void givenInFilter_whenGetCaseDefinition_thenReturnCaseDefinitions() throws SQLException {
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
    public void givenError_whenGetCaseDefinition_thenRaiseError() throws SQLException {
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
    @CsvSource({ "'2000-01-01', '2000-01-01', 1000, 7", //one day data
                 "'2000-01-01', '2001-01-31', 1000, 40", // normal flow
                 //"'', '', 0, 7", // no data
                 "'2000-01-01', '2000-01-31', 10, 30" }) // Less data than window
    public void testCalculateExtractionWindow(String initDate, String endDate, long totalCount, int expectedWindow) throws SQLException {

        //final String expectedQuery = "select  max.created_date as last_date, min.created_date as first_date \n"
        //    + "from (SELECT created_date FROM case_event CE where CE.case_type_id = 'TEST' order by created_date desc limit 1) as max,\n"
        //   + "(SELECT created_date FROM case_event CE where CE.case_type_id = 'TEST' order by created_date asc limit 1) as min";
        final String countQuery = "select count(*) \n"
            + "FROM case_event \n"
            + "WHERE case_type_id = 'TEST';";

        //if (totalCount > 0) {
        //Date init = getDate(initDate);
        //Date end = getDate(endDate);
        //when(queryExecutorFactory.provide(expectedQuery)).thenReturn(queryExecutor);
        //when(queryExecutor.execute()).thenReturn(resultSet);
        //when(resultSet.getDate(FIRST_DATE_COLUMN)).thenReturn(init);
        //when(resultSet.getDate(LAST_DATE_COLUMN)).thenReturn(end);
        //when(resultSet.next()).thenReturn(true);
        //}
        when(queryExecutorFactory.provide(countQuery)).thenReturn(additionalQueryExecutor);
        when(additionalQueryExecutor.execute()).thenReturn(additionalResultSet);
        when(additionalResultSet.next()).thenReturn(true);
        when(additionalResultSet.getLong(COUNT_COLUMN)).thenReturn(totalCount);

        assertEquals(expectedWindow, classToTest.calculateExtractionWindow(CASE_TYPE, LocalDate.parse(initDate), LocalDate.parse(endDate), true));
    }



    //    private Date getDate(String dateValue) {
    //        if (Strings.isNullOrEmpty(dateValue)) {
    //            return null;
    //        }
    //        long millis = DateTimeUtils.stringToDate(dateValue)
    //            .atStartOfDay()
    //            .atZone(ZoneId.systemDefault())
    //            .toInstant().toEpochMilli();
    //        return  new Date(millis);
    //    }
}
