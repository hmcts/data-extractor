package uk.gov.hmcts.reform.dataextractor.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import uk.gov.hmcts.reform.dataextractor.BlobOutputWriter;
import uk.gov.hmcts.reform.dataextractor.Factory;
import uk.gov.hmcts.reform.dataextractor.QueryExecutor;
import uk.gov.hmcts.reform.dataextractor.model.Output;
import uk.gov.hmcts.reform.dataextractor.service.BlobOutputValidator;
import uk.gov.hmcts.reform.dataextractor.service.Extractor;
import uk.gov.hmcts.reform.dataextractor.service.OutputStreamProvider;
import uk.gov.hmcts.reform.dataextractor.service.impl.DefaultBlobValidator;
import uk.gov.hmcts.reform.dataextractor.service.impl.ExtractorCsv;
import uk.gov.hmcts.reform.dataextractor.service.impl.ExtractorJson;
import uk.gov.hmcts.reform.dataextractor.service.impl.ExtractorJsonLines;
import uk.gov.hmcts.reform.dataextractor.service.impl.JsonValidator;

import java.time.Clock;

import static uk.gov.hmcts.reform.dataextractor.model.Output.JSON_LINES;

@Configuration
public class ApplicationConfig {

    @Autowired
    private OutputStreamProvider outputStreamProvider;

    @Autowired
    private DbConfig dbConfig;

    @Autowired
    private JsonValidator jsonLineValidator;

    @Autowired
    private DefaultBlobValidator defaultBlobValidator;

    @Bean
    public Clock clock() {
        return Clock.systemDefaultZone();
    }

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

    @Bean
    public Factory<Output, BlobOutputValidator> blobOutputValidator() {
        return this::validator;
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

    private BlobOutputValidator validator(Output outputType) {
        if (JSON_LINES.equals(outputType)) {
            return jsonLineValidator;
        }
        return defaultBlobValidator;
    }
}
