package uk.gov.hmcts.reform.dataextractor;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class QueryExecutorTest {

    private static final String JDBC_URL = "http://localhost:1234";
    private static final String USER = "test";
    private static final String PASSWORD = "test";
    private static final String SQL = "select 1";

    @InjectMocks
    public QueryExecutor classToTest;

    @Mock
    private Connection connection;
    @Mock
    private Statement statement;
    @Mock
    private ResultSet resultSet;

    @Test
    public void givenConnectionSqlException_thenSwallowException() throws SQLException {
        setConnectionInfo();
        doThrow(new SQLException()).when(connection).close();
        classToTest.close();

        verifyAllResourcesClosed();
    }

    @Test
    public void givenStatementSqlException_thenSwallowException() throws SQLException {
        setConnectionInfo();
        doThrow(new SQLException()).when(statement).close();
        classToTest.close();
        verifyAllResourcesClosed();
    }

    @Test
    public void givenResultSetSqlException_thenSwallowException() throws SQLException {
        setConnectionInfo();
        doThrow(new SQLException()).when(resultSet).close();
        classToTest.close();
        verifyAllResourcesClosed();
    }

    @Test
    public void givenEmptyConnectionInfo_thenDoNothing() throws SQLException {
        classToTest.close();
        verify(connection, never()).close();
        verify(statement, never()).close();
        verify(resultSet, never()).close();
    }

    @Test
    public void whenExecuteQuery_thenReturnDbResultSet() throws SQLException {
        classToTest = Mockito.spy(new QueryExecutor(JDBC_URL, USER, PASSWORD, SQL));
        doReturn(connection).when(classToTest).connect();
        when(connection.createStatement()).thenReturn(statement);
        when(statement.executeQuery(SQL)).thenReturn(resultSet);
        assertThat(classToTest.execute(), is(resultSet));
    }

    private void verifyAllResourcesClosed() throws SQLException {
        verify(connection, times(1)).close();
        verify(statement, times(1)).close();
        verify(resultSet, times(1)).close();
    }

    private void setConnectionInfo() {
        ReflectionTestUtils.setField(classToTest, "connection", connection);
        ReflectionTestUtils.setField(classToTest, "resultSet", resultSet);
        ReflectionTestUtils.setField(classToTest, "statement", statement);
    }
}
