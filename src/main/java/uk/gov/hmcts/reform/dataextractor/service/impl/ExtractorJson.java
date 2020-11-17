package uk.gov.hmcts.reform.dataextractor.service.impl;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import uk.gov.hmcts.reform.dataextractor.exception.ExtractorException;
import uk.gov.hmcts.reform.dataextractor.service.Extractor;

import java.io.IOException;
import java.io.OutputStream;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;


@Slf4j
@Component
@Qualifier("ExtractorJson")
public class ExtractorJson implements Extractor {

    @Autowired
    private ObjectMapper objectMapper;

    public int apply(ResultSet resultSet, OutputStream outputStream) {
        int processedData = 0;
        try (JsonGenerator jsonGenerator =
                 objectMapper.getFactory()
                     .createGenerator(outputStream, JsonEncoding.UTF8)
        ) {
            processedData = writeResultSetToJson(resultSet, jsonGenerator);
            jsonGenerator.flush();
        } catch (IOException | SQLException e) {
            throw new ExtractorException(e);
        }
        return processedData;
    }

    protected int  writeResultSetToJson(ResultSet resultSet, JsonGenerator jsonGenerator)
        throws SQLException, IOException {
        final ResultSetMetaData metaData = resultSet.getMetaData();
        final int columnCount = metaData.getColumnCount();
        jsonGenerator.writeStartArray();
        int counter = 0;
        while (resultSet.next()) {
            jsonGenerator.writeStartObject();
            for (int i = 1; i <= columnCount; i++) {
                writeRow(jsonGenerator, metaData.getColumnName(i), resultSet.getObject(i));
            }
            jsonGenerator.writeEndObject();
            counter++;
        }
        jsonGenerator.writeEndArray();
        return  counter;
    }

    protected void writeRow(JsonGenerator jsonGenerator, String columnName, Object data)
        throws IOException {
        jsonGenerator.writeObjectField(columnName, data);
    }

}
