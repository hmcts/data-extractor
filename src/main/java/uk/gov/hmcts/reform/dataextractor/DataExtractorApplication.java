package uk.gov.hmcts.reform.dataextractor;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import uk.gov.hmcts.reform.dataextractor.config.ExtractionData;
import uk.gov.hmcts.reform.dataextractor.config.Extractions;

import java.util.Locale;

@Slf4j
@SpringBootApplication
public class DataExtractorApplication implements ApplicationRunner {

    @Autowired
    private Factory<ExtractionData, BlobOutputWriter> blobOutputFactory;

    @Autowired
    private Factory<String,  QueryExecutor> queryExecutorFactory;

    @Autowired
    private Factory<DataExtractorApplication.Output,  Extractor> extractorFactory;

    @Autowired
    private Extractions extractions;

    public enum Output {
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
                .toLowerCase(Locale.ENGLISH)
                .replaceAll("[-_\\p{Space}]", "");
            if ("csv".equals(normalisedVal)) {
                return Output.CSV;
            } else if ("jsonlines".equals(normalisedVal)) {
                return Output.JSON_LINES;
            } else if ("json".equals(normalisedVal)) {
                return Output.JSON;
            } else {
                return defaultOutput();
            }
        }
    }

    @Override
    @SuppressWarnings("PMD.AvoidInstantiatingObjectsInLoops")
    public void run(ApplicationArguments args) {
        for (ExtractionData extractionData : extractions.getCaseTypes()) {
            try (QueryExecutor executor = queryExecutorFactory.provide(extractionData.getQuery());
                 BlobOutputWriter writer = blobOutputFactory.provide(extractionData))  {
                Extractor extractor = extractorFactory.provide(extractionData.getType());
                extractor.apply(executor.execute(), writer.outputStream());
            } catch (Exception e) {
                log.error("Error processing case {}", extractionData.getContainer(), e);
            }
        }
    }

    public static void main(String[] args) {
        SpringApplication.run(DataExtractorApplication.class, args);
    }

}
