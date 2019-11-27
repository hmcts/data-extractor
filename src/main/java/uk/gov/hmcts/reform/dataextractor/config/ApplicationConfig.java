package uk.gov.hmcts.reform.dataextractor.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;
import uk.gov.hmcts.reform.dataextractor.BlobOutputWriter;
import uk.gov.hmcts.reform.dataextractor.DataExtractorApplication;
import uk.gov.hmcts.reform.dataextractor.Extractor;
import uk.gov.hmcts.reform.dataextractor.ExtractorCsv;
import uk.gov.hmcts.reform.dataextractor.ExtractorJson;
import uk.gov.hmcts.reform.dataextractor.ExtractorJsonLines;
import uk.gov.hmcts.reform.dataextractor.Factory;
import uk.gov.hmcts.reform.dataextractor.OutputStreamProvider;
import uk.gov.hmcts.reform.dataextractor.QueryExecutor;

@Configuration
public class ApplicationConfig {

    @Autowired
    private OutputStreamProvider outputStreamProvider;

    @Autowired
    private DbConfig dbConfig;

    @Bean
    public Factory<ExtractionData, BlobOutputWriter> blobOutputFactory() {
        return config -> blobOutputWriter(config);
    }

    @Bean
    public Factory<String,  QueryExecutor> queryExecutorFactory() {
        return sqlQuery -> blobOutputWriter(sqlQuery);
    }

    @Bean
    public Factory<DataExtractorApplication.Output, Extractor> extractorFactory() {
        return outputType -> extractor(outputType);
    }

    @Bean
    @Scope(value = "prototype")
    public BlobOutputWriter blobOutputWriter(ExtractionData config) {
        return new BlobOutputWriter(config.getContainer(), config.getPrefix(), config.getType(), outputStreamProvider);
    }

    @Bean
    @Scope(value = "prototype")
    public QueryExecutor blobOutputWriter(String sqlQuery) {
        return new QueryExecutor(dbConfig.getUrl(), dbConfig.getUser(), dbConfig.getPassword(), sqlQuery);
    }

    public Extractor extractor(DataExtractorApplication.Output outputType) {
        switch (outputType) {
            case JSON_LINES: return new ExtractorJsonLines();
            case JSON: return new ExtractorJson();
            case CSV: return new ExtractorCsv();
            default: return extractor(DataExtractorApplication.Output.defaultOutput());
        }
    }
}
