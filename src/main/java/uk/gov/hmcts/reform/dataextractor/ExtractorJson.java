package uk.gov.hmcts.reform.dataextractor;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;


public class ExtractorJson implements Extractor {

    OutputStream terminalOutputStream;

    public void apply(ResultSet resultSet, OutputStream outputStream) {
        final ObjectMapper objectMapper = new ObjectMapper();
        try (ByteArrayOutputStream bufferedStream = new ByteArrayOutputStream(10_000_000);
            JsonGenerator jsonGenerator = objectMapper.getFactory().createGenerator(bufferedStream, JsonEncoding.UTF8)
        ) {
            this.terminalOutputStream = outputStream;
            write(resultSet, jsonGenerator, bufferedStream);
        } catch (IOException | SQLException e) {
            throw new ExtractorException(e);
        }
    }

    protected void write(ResultSet resultSet, JsonGenerator jsonGenerator, ByteArrayOutputStream bufferedStream)
        throws SQLException, IOException {
        jsonGenerator.writeStartArray();
        writeResultSetToJson(resultSet, jsonGenerator, bufferedStream);
        jsonGenerator.writeEndArray();
        jsonGenerator.flush();
        bufferedStream.writeTo(terminalOutputStream);
    }

    protected void writeResultSetToJson(
        ResultSet resultSet, JsonGenerator jsonGenerator, ByteArrayOutputStream bufferedStream)
        throws SQLException, IOException {
        final ResultSetMetaData metaData = resultSet.getMetaData();
        final int columnCount = metaData.getColumnCount();
        int iter = 0;
        while (resultSet.next()) {
            jsonGenerator.writeStartObject();
            for (int i = 1; i <= columnCount; i++) {
                writeRow(jsonGenerator, metaData.getColumnName(i), resultSet.getObject(i),
                        metaData.getColumnTypeName(i));
            }
            writeEndObject(jsonGenerator);
            iter++;
            if (iter % 200 == 0) {
                jsonGenerator.flush();
                bufferedStream.writeTo(terminalOutputStream);
                bufferedStream.reset();
            }
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
