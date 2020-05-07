package uk.gov.hmcts.reform.dataextractor.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import uk.gov.hmcts.reform.dataextractor.BlobOutputWriter;
import uk.gov.hmcts.reform.dataextractor.Factory;
import uk.gov.hmcts.reform.dataextractor.QueryExecutor;
import uk.gov.hmcts.reform.dataextractor.model.Output;
import uk.gov.hmcts.reform.dataextractor.service.Extractor;
import uk.gov.hmcts.reform.dataextractor.service.OutputStreamProvider;
import uk.gov.hmcts.reform.dataextractor.service.impl.ExtractorCsv;
import uk.gov.hmcts.reform.dataextractor.service.impl.ExtractorJson;
import uk.gov.hmcts.reform.dataextractor.service.impl.ExtractorJsonLines;

@Configuration
public class ApplicationConfig {

    @Autowired
    private OutputStreamProvider outputStreamProvider;

    @Autowired
    private DbConfig dbConfig;

    @Bean
    public Factory<ExtractionData, BlobOutputWriter> blobOutputFactory() {
        return this::blobOutputWriter;
    }

    @Bean
    public Factory<String,  QueryExecutor> queryExecutorFactory() {
        return this::queryExecutor;
    }

    @Bean
    public Factory<Output, Extractor> extractorFactory() {
        return this::extractor;
    }

    private BlobOutputWriter blobOutputWriter(ExtractionData config) {
        return new BlobOutputWriter(config.getContainer(), config.getFileName(), config.getType(), outputStreamProvider);
    }

    private QueryExecutor queryExecutor(String sqlQuery) {
        return new QueryExecutor(dbConfig.getUrl(), dbConfig.getUser(), dbConfig.getPassword(), sqlQuery);
    }

    private Extractor extractor(Output outputType) {
        switch (outputType) {
            case JSON_LINES: return new ExtractorJsonLines();
            case JSON: return new ExtractorJson();
            case CSV: return new ExtractorCsv();
            default: return extractor(Output.defaultOutput());
        }
    }
}
