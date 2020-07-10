package uk.gov.hmcts.reform.dataextractor.service.impl;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import uk.gov.hmcts.reform.dataextractor.exception.ExtractorException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ExtractorJsonTest {

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

    @Mock
    private ObjectMapper objectMapper;

    @Mock
    private JsonFactory jsonFactory;

    @Test
    void givenSqlException_thenPropagateException() throws SQLException {
        when(objectMapper.getFactory()).thenReturn(jsonFactory);
        when(resultSet.getMetaData()).thenThrow(new SQLException());
        assertThrows(ExtractorException.class, () -> classToTest.apply(resultSet, outputStream));
    }

    @Test
    void whenWriteResult_thenReturnRowsWritten() throws SQLException, IOException {
        when(resultSet.getMetaData()).thenReturn(resultSetMetaData);

        when(resultSet.next())
            .thenReturn(true)
            .thenReturn(true)
            .thenReturn(false);

        assertEquals(2, classToTest.writeResultSetToJson(resultSet, jsonGenerator), "Return expected rows");
    }

    @Test
    void testJsonGenerator() throws SQLException, IOException {
        when(resultSet.getMetaData()).thenReturn(resultSetMetaData);
        when(resultSet.next())
            .thenReturn(true)
            .thenReturn(true)
            .thenReturn(false);
        OutputStream stream = new ByteArrayOutputStream();
        JsonGenerator generator = spy(new ObjectMapper().getFactory().createGenerator(stream, JsonEncoding.UTF8));
        when(objectMapper.getFactory()).thenReturn(jsonFactory);
        when(jsonFactory.createGenerator(stream, JsonEncoding.UTF8)).thenReturn(generator);
        assertEquals(2, classToTest.apply(resultSet, stream),  "Expected extractions");
        assertEquals("[{},{}]", stream.toString(),  "Expected stream output");
        verify(generator, times(1)).flush();
    }
}
