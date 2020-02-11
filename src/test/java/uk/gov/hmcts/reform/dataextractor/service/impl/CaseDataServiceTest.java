package uk.gov.hmcts.reform.dataextractor.service.impl;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import uk.gov.hmcts.reform.dataextractor.Factory;
import uk.gov.hmcts.reform.dataextractor.QueryExecutor;
import uk.gov.hmcts.reform.dataextractor.exception.ExtractorException;

import java.sql.ResultSet;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class CaseDataServiceTest {

    @Mock
    private Factory<String, QueryExecutor> queryExecutorFactory;

    @Mock
    private QueryExecutor queryExecutor;

    @Mock
    private ResultSet resultSet;

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

}
