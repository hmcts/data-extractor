package uk.gov.hmcts.reform.dataextractor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.hmcts.reform.dataextractor.exception.ExecutorException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

@SuppressWarnings("PMD.NullAssignment")
public class QueryExecutor implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(QueryExecutor.class);
    private static final int QUERY_BATCH_SIZE = 200;

    private final String jdbcUrl;
    private final String user;
    private final String password;
    private final String sql;

    private Connection connection;
    private Statement statement;
    private ResultSet resultSet;

    public QueryExecutor(String jdbcUrl, String user, String password, String sql) {
        this.jdbcUrl = jdbcUrl;
        this.user = user;
        this.password = password;
        this.sql = sql;
    }

    Connection connect() throws SQLException {
        return DriverManager.getConnection(jdbcUrl, user, password);
    }

    @SuppressWarnings("squid:S2095")
    public ResultSet execute() {
        try {
            LOGGER.info("Connecting to db {}", jdbcUrl);
            this.connection = connect();
            this.connection.setAutoCommit(false);
            LOGGER.info("Executing sql...");
            this.statement = this.connection.createStatement();
            this.statement.setFetchSize(QUERY_BATCH_SIZE);
            long startTime = System.nanoTime();
            this.resultSet = this.statement.executeQuery(sql);
            long endTime = System.nanoTime();
            long duration = (endTime - startTime) / 1_000_000;
            LOGGER.info("Done. Execution time: {} ms", duration);
        } catch (SQLException ex) {
            throw new ExecutorException(ex);
        }
        return this.resultSet;
    }

    @Override
    public void close() {
        try {
            if (resultSet != null) {
                resultSet.close();
                resultSet = null;
                LOGGER.debug("Closed result set");
            }
        } catch (SQLException e) {
            LOGGER.warn("SQL Exception thrown while closing result set.", e);
        }
        try {
            if (statement != null) {
                statement.close();
                statement = null;
                LOGGER.debug("Closed statement");

            }
        } catch (SQLException e) {
            LOGGER.warn("SQL Exception thrown while closing statement.", e);
        }
        try {
            if (connection != null) {
                connection.close();
                connection = null;
                LOGGER.info("Closed connection");
            }
        } catch (SQLException e) {
            LOGGER.debug("SQL Exception thrown while closing connection.", e);
        }
    }

}
