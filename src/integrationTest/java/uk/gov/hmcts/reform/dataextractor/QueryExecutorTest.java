package uk.gov.hmcts.reform.dataextractor;

import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.ResultSet;

import static org.junit.jupiter.api.Assertions.assertEquals;


@Testcontainers
public class QueryExecutorTest extends DbTest {

    @Test
    public void whenSimpleSelectQueryExecuted_thenResultSetReturned() throws Exception {
        String sql = "SELECT ID, NAME FROM parent WHERE ID = 1";
        try (QueryExecutor executor = new QueryExecutor(jdbcUrl, username, password, sql);
             ResultSet resultSet = executor.execute()) {
            boolean next = resultSet.next();
            assertEquals(true, next);
            int result = resultSet.getInt(1);
            assertEquals(1, result);
        }
    }

}
