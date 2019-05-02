package uk.gov.hmcts.reform.dataextractor;

import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.ByteArrayOutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;

import static org.junit.jupiter.api.Assertions.assertEquals;


@Testcontainers
public class ExtractorJsonTest extends DbTest {


    @Test
    public void whenSimpleSelectQueryExecuted_thenJsonReturned() throws Exception {
        try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password);
            ResultSet resultSet = conn.createStatement().executeQuery("SELECT ID, NAME FROM parent WHERE ID = 1")) {

            ExtractorJson extractor = new ExtractorJson();
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            extractor.apply(resultSet, out);
            assertEquals("[{\"id\":1,\"name\":\"A\"}]", out.toString());
        }
    }

    @Test
    public void whenJoinSelectQueryExecuted_thenJsonResultsReturned() throws Exception {
        try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password);
            ResultSet resultSet =
                conn.createStatement().executeQuery(
                    "SELECT P.ID, P.NAME, C.ID as \"child id\", C.NAME as \"child,name\" "
                        + "FROM parent P JOIN child C on P.ID = C.PARENT_ID WHERE P.ID = 1")) {

            ExtractorJson extractor = new ExtractorJson();
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            extractor.apply(resultSet, out);
            assertEquals(
                "[{\"id\":1,\"name\":\"A\",\"child id\":1,\"child,name\":\"A1\"},"
                        + "{\"id\":1,\"name\":\"A\",\"child id\":2,\"child,name\":\"A2\"}]",
                out.toString()
            );
        }

    }

}
