package uk.gov.hmcts.reform.dataextractor;

import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;
import uk.gov.hmcts.reform.dataextractor.utils.TestUtils;

import java.io.ByteArrayOutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;

import static org.junit.jupiter.api.Assertions.assertEquals;


@Testcontainers
public class ExtractorJsonLinesTest extends DbTest {

    @Test
    public void whenSimpleSelectQueryExecuted_thenJsonLinesReturned() throws Exception {
        try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password);
             ResultSet resultSet = conn.createStatement()
                     .executeQuery("SELECT ID, NAME FROM parent WHERE ID IN (1, 2)")) {

            ExtractorJson extractor = new ExtractorJsonLines();
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            extractor.apply(resultSet, out);
            assertEquals("{\"id\":1,\"name\":\"A\"}\n{\"id\":2,\"name\":\"B\"}\n", out.toString());
        }
    }

    @Test
    public void whenJoinSelectQueryWithRawJsonExecuted_thenJsonLinesResultsReturned() throws Exception {
        try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password);
             ResultSet resultSet =
                 conn.createStatement().executeQuery(
                     "SELECT P.ID, P.NAME, C.ID as \"child id\", C.DATA as data, C.NAME as \"child name\" "
                         + "FROM parent P JOIN child C on P.ID = C.PARENT_ID WHERE P.ID = 1")) {

            ExtractorJson extractor = new ExtractorJsonLines();
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            extractor.apply(resultSet, out);
            assertEquals(TestUtils.getDataFromFile("joinSelectQueryExpectedResult.json"), out.toString());
        }

    }

}
