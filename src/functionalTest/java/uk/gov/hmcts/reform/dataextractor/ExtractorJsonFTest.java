package uk.gov.hmcts.reform.dataextractor;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.testcontainers.junit.jupiter.Testcontainers;

import uk.gov.hmcts.reform.dataextractor.service.impl.ExtractorJson;

import java.io.ByteArrayOutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static uk.gov.hmcts.reform.dataextractor.utils.TestConstants.DB_CONNECTION_QUERY;
import static uk.gov.hmcts.reform.dataextractor.utils.TestConstants.DB_DATA_QUERY;


@Testcontainers
@ExtendWith(SpringExtension.class)
@ActiveProfiles(profiles = "test")
@SpringBootTest(classes = TestApplicationConfiguration.class)
public class ExtractorJsonFTest extends DbTest {

    @Autowired
    ExtractorJson extractorJson;

    @Test
    public void whenSimpleSelectQueryExecuted_thenJsonReturned() throws Exception {
        try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password);
            ResultSet resultSet = conn.createStatement().executeQuery(DB_CONNECTION_QUERY)) {

            ByteArrayOutputStream out = new ByteArrayOutputStream();
            extractorJson.apply(resultSet, out);
            assertEquals("[{\"id\":1,\"name\":\"A\"}]", out.toString());
        }
    }

    @Test
    public void whenJoinSelectQueryExecuted_thenJsonResultsReturned() throws Exception {
        try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password);
            ResultSet resultSet =
                conn.createStatement().executeQuery(DB_DATA_QUERY)) {

            ByteArrayOutputStream out = new ByteArrayOutputStream();
            extractorJson.apply(resultSet, out);
            assertEquals(
                "[{\"id\":1,\"name\":\"A\",\"child id\":1,\"child,name\":\"A1\"},"
                        + "{\"id\":1,\"name\":\"A\",\"child id\":2,\"child,name\":\"A2\"}]",
                out.toString()
            );
        }

    }

}
