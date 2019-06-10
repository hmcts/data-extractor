package uk.gov.hmcts.reform.dataextractor;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;


@SuppressWarnings({"PMD", "checkstyle:hideutilityclassconstructor"})
public class DataExtractorApplication {

    enum Output {
        CSV("csv", "text/csv"),
        JSON("json", "application/json"),
        JSON_LINES("jsonl", "application/x-ndjson");
        // See: https://github.com/wardi/jsonlines/issues/9

        private String extension;
        private String applicationContent;

        Output(String extension, String applicationContent) {
            this.extension = extension;
            this.applicationContent = applicationContent;
        }

        public String getExtension() {
            return extension;
        }

        public String getApplicationContent() {
            return applicationContent;
        }

        public static Output defaultOutput() {
            return Output.JSON_LINES;
        }

        public static Output from(String val) {
            if (val == null) {
                return defaultOutput();
            }
            String normalisedVal = val
                .toLowerCase()
                .replaceAll("[-_\\p{Space}]", "");
            if (normalisedVal.equals("csv")) {
                return Output.CSV;
            } else if (normalisedVal.equals("jsonlines")) {
                return Output.JSON_LINES;
            } else if (normalisedVal.equals("json")) {
                return Output.JSON;
            } else {
                return defaultOutput();
            }
        }
    }

    static class ExtractorConfig {
        private String baseDir = "/mnt/secrets/";

        private final String etlDbUrl;
        final String etlDbUser;
        final String etlDbPassword;
        private final String etlSql;
        private final String etlMsiClientId;
        private final String etlAccount;
        private final String etlContainer;
        private final Output etlFileType;
        private final String etlFilePrefix;

        ExtractorConfig(String baseDir) {
            if (baseDir != null) {
                this.baseDir = baseDir;
            }
            Config config = ConfigFactory.load();
            this.etlDbUrl = config.getString("etl-db-url");
            if (config.hasPath("etl-db-user-file")) {
                String etlDbUserFile = config.getString("etl-db-user-file");
                this.etlDbUser = readFirstLine(etlDbUserFile);
            } else {
                this.etlDbUser = config.getString("etl-db-user");
            }
            if (config.hasPath("etl-db-password-file")) {
                String etlDbPasswordFile = config.getString("etl-db-password-file");
                this.etlDbPassword = readFirstLine(etlDbPasswordFile);
            } else {
                this.etlDbPassword = config.getString("etl-db-password");
            }
            this.etlSql = config.getString("etl-sql");
            this.etlMsiClientId = config.getString("etl-msi-client-id");
            this.etlAccount = config.getString("etl-account");
            this.etlContainer = config.getString("etl-container");
            if (config.hasPath("etl-file-type")) {
                this.etlFileType = Output.from(config.getString("etl-file-type"));
            } else {
                this.etlFileType = Output.defaultOutput();
            }
            this.etlFilePrefix = config.getString("etl-file-prefix");
        }

        ExtractorConfig() {
            this(null);
        }

        private String readFirstLine(String fileName) {
            try {
                return Files.readAllLines(Paths.get(baseDir, fileName))
                    .stream()
                    .findFirst()
                    .orElseThrow();
            } catch (IOException e) {
                throw new ExtractorException(e);
            }
        }
    }


    private ExtractorConfig config;


    public DataExtractorApplication() {
        this.config = new ExtractorConfig();
    }

    DataExtractorApplication(String baseDir) {
        this.config = new ExtractorConfig(baseDir);
    }

    protected static Extractor extractorFactory(Output outputType) {
        switch (outputType) {
            case JSON_LINES: return new ExtractorJsonLines();
            case JSON: return new ExtractorJson();
            case CSV: return new ExtractorCsv();
            default: return extractorFactory(Output.defaultOutput());
        }
    }

    public void run() {
        try (QueryExecutor executor = new QueryExecutor(
                config.etlDbUrl, config.etlDbUser, config.etlDbPassword, config.etlSql);
            BlobOutputWriter writer = new BlobOutputWriter(
                config.etlMsiClientId, config.etlAccount, config.etlContainer, config.etlFilePrefix, config.etlFileType)
            ) {
            Extractor extractor = DataExtractorApplication.extractorFactory(config.etlFileType);
            extractor.apply(executor.execute(), writer.outputStream());
        }
    }

    public ExtractorConfig getConfig() {
        return config;
    }


    public static void main(String[] args) {
        new DataExtractorApplication().run();
    }

}
