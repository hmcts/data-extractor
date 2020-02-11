package uk.gov.hmcts.reform.dataextractor;

import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

import uk.gov.hmcts.reform.dataextractor.service.impl.ExtractorCsv;

import java.io.ByteArrayOutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static uk.gov.hmcts.reform.dataextractor.utils.TestConstants.DB_CONNECTION_QUERY;
import static uk.gov.hmcts.reform.dataextractor.utils.TestConstants.DB_DATA_QUERY;


@Testcontainers
public class ExtractorCsvFTest extends DbTest {


    @Test
    public void whenSimpleSelectQueryExecuted_thenCsvResultsReturned() throws Exception {
        try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password);
            ResultSet resultSet = conn.createStatement().executeQuery(DB_CONNECTION_QUERY)) {

            ExtractorCsv extractor = new ExtractorCsv();
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            extractor.apply(resultSet, out);
            assertEquals("id,name\r\n1,A\r\n", out.toString());
        }
    }

    @Test
    public void whenJoinSelectQueryExecuted_thenCsvResultsReturned() throws Exception {
        try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password);
            ResultSet resultSet =
                conn.createStatement().executeQuery(DB_DATA_QUERY)) {

            ExtractorCsv extractor = new ExtractorCsv();
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            extractor.apply(resultSet, out);
            assertEquals("id,name,child id,\"child,name\"\r\n1,A,1,A1\r\n1,A,2,A2\r\n", out.toString());
        }
    }

}
