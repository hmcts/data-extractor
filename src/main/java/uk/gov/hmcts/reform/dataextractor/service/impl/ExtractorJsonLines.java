package uk.gov.hmcts.reform.dataextractor.service.impl;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.util.MinimalPrettyPrinter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;


@Slf4j
@Component
@Qualifier("ExtractorJsonLines")
public class ExtractorJsonLines extends ExtractorJson {

    @Override
    protected int writeResultSetToJson(ResultSet resultSet, JsonGenerator jsonGenerator)
        throws SQLException, IOException {
        final ResultSetMetaData metaData = resultSet.getMetaData();
        final int columnCount = metaData.getColumnCount();
        jsonGenerator.setPrettyPrinter(new MinimalPrettyPrinter(""));
        int counter = 0;
        while (resultSet.next()) {
            jsonGenerator.writeStartObject();
            for (int i = 1; i <= columnCount; i++) {
                writeRow(jsonGenerator, metaData.getColumnName(i), resultSet.getObject(i),
                        metaData.getColumnTypeName(i));
            }
            jsonGenerator.writeEndObject();
            jsonGenerator.writeRaw('\n');

            counter++;
        }
        return counter;
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
