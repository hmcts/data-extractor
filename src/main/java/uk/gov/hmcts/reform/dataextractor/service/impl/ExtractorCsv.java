package uk.gov.hmcts.reform.dataextractor.service.impl;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import uk.gov.hmcts.reform.dataextractor.exception.ExtractorException;
import uk.gov.hmcts.reform.dataextractor.service.Extractor;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.sql.ResultSet;
import java.sql.SQLException;


public class ExtractorCsv implements Extractor {

    public void apply(ResultSet resultSet, OutputStream outputStream) {
        try (CSVPrinter printer =
            new CSVPrinter(new PrintWriter(outputStream, false), CSVFormat.DEFAULT.withHeader(resultSet))
        ) {
            printer.printRecords(resultSet);
            printer.flush();
        } catch (IOException | SQLException e) {
            throw new ExtractorException(e);
        }
    }

}
