package uk.gov.hmcts.reform.dataextractor;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
public class QueryExecutorTest {

    @InjectMocks
    public QueryExecutor classToTest;

    @Mock
    private Connection connection;
    @Mock
    private Statement statement;
    @Mock
    private ResultSet resultSet;

    @BeforeEach
    public void setup() {
        ReflectionTestUtils.setField(classToTest, "connection", connection);
        ReflectionTestUtils.setField(classToTest, "resultSet", resultSet);
        ReflectionTestUtils.setField(classToTest, "statement", statement);
    }

    @Test
    public void givenConnectionSqlException_thenSwallowException() throws SQLException {
        doThrow(new SQLException()).when(connection).close();
        classToTest.close();

        verifyAllResourcesClosed();
    }

    @Test
    public void givenStatementSqlException_thenSwallowException() throws SQLException {
        doThrow(new SQLException()).when(statement).close();
        classToTest.close();
        verifyAllResourcesClosed();
    }

    @Test
    public void givenResultSetSqlException_thenSwallowException() throws SQLException {
        doThrow(new SQLException()).when(resultSet).close();
        classToTest.close();
        verifyAllResourcesClosed();
    }

    private void verifyAllResourcesClosed() throws SQLException {
        verify(connection, times(1)).close();
        verify(statement, times(1)).close();
        verify(resultSet, times(1)).close();
    }
}
