package uk.gov.hmcts.reform.dataextractor.service.impl;

import com.fasterxml.jackson.core.JsonGenerator;
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class ExtractorJsonTest {

    @InjectMocks
    public ExtractorJson classToTest;

    @Mock
    private ResultSet resultSet;

    @Mock
    private OutputStream outputStream;

    @Mock
    private ResultSetMetaData resultSetMetaData;

    @Mock
    private JsonGenerator jsonGenerator;

    @Test
    public void givenSqlException_thenPropagateException() throws SQLException {
        when(resultSet.getMetaData()).thenThrow(new SQLException());
        assertThrows(ExtractorException.class, () -> classToTest.apply(resultSet, outputStream));
    }

    @Test
    public void whenWriteResult_thenReturnRowsWritten() throws SQLException, IOException {
        when(resultSet.getMetaData()).thenReturn(resultSetMetaData);

        when(resultSet.next())
            .thenReturn(true)
            .thenReturn(true)
            .thenReturn(false);

        assertEquals(2, classToTest.writeResultSetToJson(resultSet, jsonGenerator), "Return expected rows");
    }
}