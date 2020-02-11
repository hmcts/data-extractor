package uk.gov.hmcts.reform.dataextractor;

import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

import uk.gov.hmcts.reform.dataextractor.service.impl.ExtractorJson;
import uk.gov.hmcts.reform.dataextractor.service.impl.ExtractorJsonLines;
import uk.gov.hmcts.reform.dataextractor.utils.TestUtils;

import java.io.ByteArrayOutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;

import static org.junit.jupiter.api.Assertions.assertEquals;


@Testcontainers
public class ExtractorJsonLinesFTest extends DbTest {


    @Test
    public void whenSimpleSelectQueryExecuted_thenJsonLinesReturned() throws Exception {
        try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password);
             ResultSet resultSet = conn.createStatement()
                     .executeQuery("SELECT ID, NAME FROM case_data WHERE ID IN (1, 2)")) {

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
                 conn.createStatement().executeQuery("SELECT P.ID, P.NAME, C.ID as \"child id\", C.NAME as \"child.name\", C.DATA "
                     + "FROM case_data P JOIN case_event C on P.ID = C.case_data_id WHERE P.ID = 1")) {

            ExtractorJson extractor = new ExtractorJsonLines();
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            extractor.apply(resultSet, out);
            assertEquals(TestUtils.getDataFromFile("joinSelectQueryExpectedResult.json"), out.toString());
        }

    }

}
