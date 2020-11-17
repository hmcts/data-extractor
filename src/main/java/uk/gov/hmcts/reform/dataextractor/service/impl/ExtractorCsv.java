package uk.gov.hmcts.reform.dataextractor.service.impl;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.springframework.stereotype.Component;

import uk.gov.hmcts.reform.dataextractor.exception.ExtractorException;
import uk.gov.hmcts.reform.dataextractor.service.Extractor;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.sql.Clob;
import java.sql.ResultSet;
import java.sql.SQLException;


@Component
public class ExtractorCsv implements Extractor {

    public int apply(ResultSet resultSet, OutputStream outputStream) {
        int count = 0;
        try (CSVPrinter printer = new CSVPrinter(new PrintWriter(outputStream, false), CSVFormat.DEFAULT.withHeader(resultSet))) {
            count = printRecordsWithCounter(printer, resultSet);
            printer.flush();
        } catch (IOException | SQLException e) {
            throw new ExtractorException(e);
        }
        return count;
    }

    private int printRecordsWithCounter(final CSVPrinter printer, final ResultSet resultSet) throws SQLException, IOException {
        final int columnCount = resultSet.getMetaData().getColumnCount();
        int count = 0;
        while (resultSet.next()) {
            for (int i = 1; i <= columnCount; i++) {
                final Object object = resultSet.getObject(i);
                printer.print(object instanceof Clob ? ((Clob) object).getCharacterStream() : object);
            }
            printer.println();
            count++;
        }
        return count;
    }
}
