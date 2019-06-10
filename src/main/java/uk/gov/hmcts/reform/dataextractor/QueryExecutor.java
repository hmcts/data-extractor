package uk.gov.hmcts.reform.dataextractor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;


public class QueryExecutor implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(QueryExecutor.class);

    private final String jdbcUrl;
    private final String user;
    private final String password;
    private final String sql;

    private Connection connection;
    private ResultSet resultSet;

    public QueryExecutor(String jdbcUrl, String user, String password, String sql) {
        this.jdbcUrl = jdbcUrl;
        this.user = user;
        this.password = password;
        this.sql = sql;
    }

    private Connection connect() throws SQLException {
        return DriverManager.getConnection(jdbcUrl, user, password);
    }

    @SuppressWarnings("squid:S2095")
    public ResultSet execute() {
        try {
            LOGGER.info("Connecting to db {}", jdbcUrl);
            this.connection = connect();
            LOGGER.info("Executing sql...");
            long startTime = System.nanoTime();
            this.resultSet = this.connection.createStatement().executeQuery(sql);
            long endTime = System.nanoTime();
            long duration = (endTime - startTime) / 1_000_000;
            LOGGER.info("Done. Execution time: {} ms", duration);
        } catch (SQLException ex) {
            throw new ExecutorException(ex);
        }
        return this.resultSet;
    }

    public void close() {
        try {
            resultSet.close();
        } catch (SQLException e) {
            LOGGER.warn("SQL Exception thrown while closing result set.", e);
        }
        try {
            connection.close();
        } catch (SQLException e) {
            LOGGER.warn("SQL Exception thrown while closing connection.", e);
        }
    }

}
