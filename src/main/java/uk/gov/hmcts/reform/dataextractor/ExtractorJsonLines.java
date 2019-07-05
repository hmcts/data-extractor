package uk.gov.hmcts.reform.dataextractor;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.util.MinimalPrettyPrinter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;


public class ExtractorJsonLines extends ExtractorJson {

    @Override
    protected void write(ResultSet resultSet, JsonGenerator jsonGenerator, ByteArrayOutputStream bufferedStream)
        throws SQLException, IOException {
        jsonGenerator.setPrettyPrinter(new MinimalPrettyPrinter(""));
        writeResultSetToJson(resultSet, jsonGenerator, bufferedStream);
        jsonGenerator.flush();
        bufferedStream.writeTo(terminalOutputStream);
    }

    @Override
    protected void writeEndObject(JsonGenerator jsonGenerator) throws IOException {
        jsonGenerator.writeEndObject();
        jsonGenerator.writeRaw("\n");
    }

    protected void writeRow(JsonGenerator jsonGenerator, String columnName, Object data, String dataType)
        throws IOException {
        if (dataType.contains("json")) {
            jsonGenerator.writeFieldName(columnName);
            jsonGenerator.writeRawValue(data.toString());
        } else {
            jsonGenerator.writeObjectField(columnName, data);
        }
    }

}
