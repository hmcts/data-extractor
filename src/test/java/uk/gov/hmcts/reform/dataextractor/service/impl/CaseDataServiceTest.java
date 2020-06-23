package uk.gov.hmcts.reform.dataextractor.service.impl;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import uk.gov.hmcts.reform.dataextractor.Factory;
import uk.gov.hmcts.reform.dataextractor.QueryExecutor;
import uk.gov.hmcts.reform.dataextractor.config.ExtractionFilters;
import uk.gov.hmcts.reform.dataextractor.exception.ExtractorException;
import uk.gov.hmcts.reform.dataextractor.model.CaseDefinition;

import java.sql.ResultSet;
import java.sql.SQLException;
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
}
