package uk.gov.hmcts.reform.dataextractor.service.impl;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.extern.slf4j.Slf4j;
import uk.gov.hmcts.reform.dataextractor.exception.ExtractorException;
import uk.gov.hmcts.reform.dataextractor.service.Extractor;

import java.io.IOException;
import java.io.OutputStream;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;


@Slf4j
public class ExtractorJson implements Extractor {

    public void apply(ResultSet resultSet, OutputStream outputStream) {
        final ObjectMapper objectMapper = new ObjectMapper();
        try (JsonGenerator jsonGenerator =
            objectMapper.getFactory()
            .createGenerator(outputStream, JsonEncoding.UTF8)
        ) {
            int processedData = writeResultSetToJson(resultSet, jsonGenerator);
            jsonGenerator.flush();
            log.info("Total data processed in current batch: {}", processedData);
        } catch (IOException | SQLException e) {
            throw new ExtractorException(e);
        }
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
