package uk.gov.hmcts.reform.dataextractor;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertEquals;


@Testcontainers
public class QueryExecutorTest extends DbTest {

    Connection dbConnection;

    @BeforeEach
    public void setup() throws SQLException {
        dbConnection = DriverManager.getConnection(jdbcUrl, username, password);
    }

    @AfterEach
    public void tearDown() throws SQLException {
        dbConnection.close();
    }

    @Test
    public void whenSimpleSelectQueryExecuted_thenResultSetReturned() throws Exception {
        String sql = "SELECT ID, NAME FROM parent WHERE ID = 1";
        try (QueryExecutor executor = new QueryExecutor(dbConnection, sql);
             ResultSet resultSet = executor.execute()) {
            boolean next = resultSet.next();
            assertEquals(true, next);
            int result = resultSet.getInt(1);
            assertEquals(1, result);
        }
    }

    @Test
    public void whenBadSelectQueryExecuted_thenExceptionThrown() throws Exception {
        String sql = "SELECT ID, NAME FROM parent WHERE PID = 1";
        Assertions.assertThrows(ExecutorException.class, () -> {
            try (QueryExecutor executor = new QueryExecutor(dbConnection, sql);
                 ResultSet resultSet = executor.execute()) {
                boolean next = resultSet.next();
                assertEquals(true, next);
                int result = resultSet.getInt(1);
                assertEquals(1, result);
            }
        });
    }

}
