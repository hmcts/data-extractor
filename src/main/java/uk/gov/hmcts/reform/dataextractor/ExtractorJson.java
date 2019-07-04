package uk.gov.hmcts.reform.dataextractor;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.OutputStream;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;


public class ExtractorJson implements Extractor {

    public void apply(ResultSet resultSet, OutputStream outputStream) {
        final ObjectMapper objectMapper = new ObjectMapper();
        try (JsonGenerator jsonGenerator = objectMapper.getFactory()
            .configure(JsonGenerator.Feature.FLUSH_PASSED_TO_STREAM, false)
            .createGenerator(outputStream, JsonEncoding.UTF8)
        ) {
            write(resultSet, jsonGenerator);
        } catch (IOException | SQLException e) {
            throw new ExtractorException(e);
        }
    }

    protected void write(ResultSet resultSet, JsonGenerator jsonGenerator)
        throws SQLException, IOException {
        jsonGenerator.writeStartArray();
        writeResultSetToJson(resultSet, jsonGenerator);
        jsonGenerator.writeEndArray();
    }

    protected void writeResultSetToJson(ResultSet resultSet, JsonGenerator jsonGenerator)
        throws SQLException, IOException {
        final ResultSetMetaData metaData = resultSet.getMetaData();
        final int columnCount = metaData.getColumnCount();
        while (resultSet.next()) {
            jsonGenerator.writeStartObject();
            for (int i = 1; i <= columnCount; i++) {
                writeRow(jsonGenerator, metaData.getColumnName(i), resultSet.getObject(i),
                        metaData.getColumnTypeName(i));
            }
            writeEndObject(jsonGenerator);
        }
    }

    protected void writeEndObject(JsonGenerator jsonGenerator) throws IOException {
        jsonGenerator.writeEndObject();
    }

    protected void writeRow(JsonGenerator jsonGenerator, String columnName, Object data, String dataType)
        throws IOException {
        jsonGenerator.writeObjectField(columnName, data);
    }

}
