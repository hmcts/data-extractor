package uk.gov.hmcts.reform.dataextractor;

import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;

import static org.junit.jupiter.api.Assertions.assertEquals;


@Testcontainers
public class SelectTest extends DbTest {

    @Test
    public void whenSelectQueryExecuted_thenResultsReturned() throws Exception {
        try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password);
            ResultSet resultSet = conn.createStatement().executeQuery("SELECT ID FROM parent WHERE ID = 1")) {

            resultSet.next();
            int result = resultSet.getInt(1);
            assertEquals(1, result);
        }
    }

}
