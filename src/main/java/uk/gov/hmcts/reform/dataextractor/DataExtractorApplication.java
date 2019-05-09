package uk.gov.hmcts.reform.dataextractor;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;


@SuppressWarnings({"PMD", "checkstyle:hideutilityclassconstructor"})
public class DataExtractorApplication {

    enum Output {
        CSV("csv"),
        JSON("json"),
        JSON_LINES("json_lines");

        private String label;

        Output(String label) {
            if (label != null) {
                this.label = label.toLowerCase();
            }
        }

        public static Output defaultOutput() {
            return Output.JSON_LINES;
        }
    }

    private static class ExtractorConfig {
        private final String etlDbUrl;
        private final String etlDbUser;
        private final String etlDbPassword;
        private final String etlSql;
        private final String etlContainer;
        private final String etlAccount;


        public ExtractorConfig() {
            Config config = ConfigFactory.load();
            this.etlDbUrl = config.getString("etl-db-url");
            this.etlDbUser = config.getString("etl-db-user");
            this.etlDbPassword = config.getString("etl-db-password");
            this.etlSql = config.getString("etl-sql");
            this.etlAccount = config.getString("etl-account");
            this.etlContainer = config.getString("etl-container");
        }
    }


    private ExtractorConfig extractorConfig;


    public static void main(String[] args) {
        DataExtractorApplication application = new DataExtractorApplication();
        application.extractorConfig = new ExtractorConfig();
    }

}
