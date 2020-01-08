package uk.gov.hmcts.reform.dataextractor;

import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.ByteArrayOutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;

import static org.junit.jupiter.api.Assertions.assertEquals;


@Testcontainers
public class ExtractorCsvTest extends DbTest {


    @Test
    public void whenSimpleSelectQueryExecuted_thenCsvResultsReturned() throws Exception {
        try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password);
            ResultSet resultSet = conn.createStatement().executeQuery("SELECT ID, NAME FROM parent WHERE ID = 1")) {

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
                conn.createStatement().executeQuery(
                    "SELECT P.ID, P.NAME, C.ID as \"child id\", C.NAME as \"child,name\" "
                        + "FROM parent P JOIN child C on P.ID = C.PARENT_ID WHERE P.ID = 1")) {

            ExtractorCsv extractor = new ExtractorCsv();
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            extractor.apply(resultSet, out);
            assertEquals("id,name,child id,\"child,name\"\r\n1,A,1,A1\r\n1,A,2,A2\r\n", out.toString());
        }
    }

}
