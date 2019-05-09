package uk.gov.hmcts.reform.dataextractor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;


public class QueryExecutor implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(QueryExecutor.class);

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

    public ResultSet execute() {
        try {
            this.connection = connect();
            this.resultSet = this.connection.createStatement().executeQuery(sql);
        } catch (SQLException ex) {
            throw new ExtractorException(ex);
        }
        return this.resultSet;
    }

    public void close() {
        try {
            resultSet.close();
        } catch (SQLException e) {
            log.warn("SQL Exception thrown while closing result set.", e);
        }
        try {
            connection.close();
        } catch (SQLException e) {
            log.warn("SQL Exception thrown while closing connection.", e);
        }
    }

}
