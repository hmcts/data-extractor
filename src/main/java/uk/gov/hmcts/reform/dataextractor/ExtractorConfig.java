package uk.gov.hmcts.reform.dataextractor;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;


public class ExtractorConfig {

    private final Config config;
    private final String etlDbUrl;
    private final String etlDbUser;
    private final String etlDbPassword;
    private final String etlSql;
    private final String etlContainer;
    private final String etlAccount;


    public ExtractorConfig() {
        this.config = ConfigFactory.load();
        this.etlDbUrl = config.getString("etl-db-url");
        this.etlDbUser = config.getString("etl-db-user");
        this.etlDbPassword = config.getString("etl-db-password");
        this.etlSql = config.getString("etl-sql");
        this.etlAccount = config.getString("etl-account");
        this.etlContainer = config.getString("etl-container");
    }


    public String getEtlDbUrl() {
        return etlDbUrl;
    }

    public String getEtlDbUser() {
        return etlDbUser;
    }

    public String getEtlDbPassword() {
        return etlDbPassword;
    }

    public String getEtlSql() {
        return etlSql;
    }

    public String getEtlContainer() {
        return etlContainer;
    }

    public String getEtlAccount() {
        return etlAccount;
    }

}
