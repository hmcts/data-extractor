package uk.gov.hmcts.reform.dataextractor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;


public class QueryExecutor implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(QueryExecutor.class);
    private static final int QUERY_BATCH_SIZE = 200;

    private final String sql;
    private final  Connection connection;

    private Statement statement;
    private ResultSet resultSet;

    public QueryExecutor(Connection connection, String sql) {
        this.sql = sql;
        this.connection = connection;
    }

    @SuppressWarnings("squid:S2095")
    public ResultSet execute() {
        try {
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

    public void close() {
        try {
            resultSet.close();
        } catch (SQLException e) {
            LOGGER.warn("SQL Exception thrown while closing result set.", e);
        }
        try {
            statement.close();
        } catch (SQLException e) {
            LOGGER.warn("SQL Exception thrown while closing statement.", e);
        }
    }

}
