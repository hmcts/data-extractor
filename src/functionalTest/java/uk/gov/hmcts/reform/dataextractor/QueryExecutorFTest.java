package uk.gov.hmcts.reform.dataextractor;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

import uk.gov.hmcts.reform.dataextractor.exception.ExecutorException;

import java.sql.ResultSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static uk.gov.hmcts.reform.dataextractor.utils.TestConstants.DB_CONNECTION_QUERY;


@Testcontainers
public class QueryExecutorFTest extends DbTest {

    @Test
    void whenSimpleSelectQueryExecuted_thenResultSetReturned() throws Exception {
        String sql = DB_CONNECTION_QUERY;
        try (QueryExecutor executor = new QueryExecutor(jdbcUrl, username, password, sql);
             ResultSet resultSet = executor.execute()) {
            boolean next = resultSet.next();
            assertEquals(true, next);
            int result = resultSet.getInt(1);
            assertEquals(1, result);
        }
    }

    @Test
    void whenBadSelectQueryExecuted_thenExceptionThrown() throws Exception {
        String sql = "SELECT ID, NAME FROM parent WHERE PID = 1";
        Assertions.assertThrows(ExecutorException.class, () -> {
            try (QueryExecutor executor = new QueryExecutor(jdbcUrl, username, password, sql);
                 ResultSet resultSet = executor.execute()) {
                boolean next = resultSet.next();
                assertEquals(true, next);
                int result = resultSet.getInt(1);
                assertEquals(1, result);
            }
        });
    }

}
