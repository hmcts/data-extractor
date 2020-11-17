package uk.gov.hmcts.reform.dataextractor.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
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
import uk.gov.hmcts.reform.dataextractor.utils.MiTimestampSerializer;

import java.sql.Timestamp;
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

    @Autowired
    private ExtractorJsonLines extractorJsonLines;

    @Autowired
    @Qualifier("ExtractorJson")
    private ExtractorJson extractorJson;

    @Autowired
    private ExtractorCsv extractorCsv;

    @Value("${extraction.initialise:false}")
    private boolean initialise;

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

    @Bean
    public ObjectMapper objectMapper(MiTimestampSerializer serializer) {
        SimpleModule module = new SimpleModule("Data serializer");
        module.addSerializer(Timestamp.class, serializer);
        return new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .registerModule(module)
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    private BlobOutputWriter blobOutputWriter(ExtractionData config) {
        return new BlobOutputWriter(config.getContainer(), config.getFileName(), config.getType(), outputStreamProvider);
    }

    private QueryExecutor queryExecutor(String sqlQuery) {
        if (initialise) {
            return new QueryExecutor(dbConfig.getCloneUrl(), dbConfig.getCloneUser(), dbConfig.getClonePassword(), sqlQuery);
        }
        return new QueryExecutor(dbConfig.getUrl(), dbConfig.getUser(), dbConfig.getPassword(), sqlQuery);
    }

    private Extractor extractor(Output outputType) {
        switch (outputType) {
            case JSON_LINES: return extractorJsonLines;
            case JSON: return extractorJson;
            case CSV: return extractorCsv;
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
