package uk.gov.hmcts.reform.dataextractor.service.impl;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import uk.gov.hmcts.reform.dataextractor.exception.ExtractorException;

import java.io.IOException;
import java.io.OutputStream;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class ExtractorCsvTest {

    @InjectMocks
    public ExtractorCsv classToTest;

    @Mock
    private ResultSet resultSet;

    @Mock
    private OutputStream outputStream;

    @Mock
    private ResultSetMetaData resultSetMetaData;

    @Test
    public void givenSqlException_thenPropagateException() throws SQLException, IOException {
        when(resultSet.getMetaData()).thenReturn(resultSetMetaData);
        when(resultSet.next()).thenThrow(new SQLException());
        assertThrows(ExtractorException.class, () -> classToTest.apply(resultSet, outputStream));
        verify(outputStream, times(1)).close();
    }

    @Test
    public void givenSqlException_AndCloseException_thenPropagateException() throws SQLException, IOException {
        when(resultSet.getMetaData()).thenReturn(resultSetMetaData);
        when(resultSet.next()).thenThrow(new SQLException());
        doThrow(new IOException("Test exception")).when(outputStream).close();
        assertThrows(ExtractorException.class, () -> classToTest.apply(resultSet, outputStream));
        verify(outputStream, times(1)).close();
    }
}
