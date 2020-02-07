package uk.gov.hmcts.reform.dataextractor;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import uk.gov.hmcts.reform.dataextractor.config.ExtractionData;
import uk.gov.hmcts.reform.dataextractor.config.Extractions;
import uk.gov.hmcts.reform.dataextractor.model.Output;
import uk.gov.hmcts.reform.dataextractor.service.Extractor;
import uk.gov.hmcts.reform.dataextractor.service.impl.BlobServiceImpl;

import java.sql.ResultSet;
import java.time.LocalDate;

@Component
@Slf4j
public class ExtractionComponent {
    @Autowired
    private Factory<ExtractionData, BlobOutputWriter> blobOutputFactory;

    @Autowired
    private Factory<String,  QueryExecutor> queryExecutorFactory;

    @Autowired
    private Factory<Output, Extractor> extractorFactory;

    @Autowired
    private Extractions extractions;

    @Autowired
    private BlobServiceImpl blobService;

    @SuppressWarnings("PMD.CloseResource")
    public void execute() {
        for (ExtractionData extractionData : extractions.getCaseTypes()) {
            log.info("Processing data for caseType {} with prefix {}", extractionData.getContainer(), extractionData.getPrefix());
            LocalDate lastUpdated = blobService.getContainerLastUpdated(extractionData.getContainer());

            String query = QueryBuilder
                .builder()
                .fromDate(lastUpdated)
                .extractionData(extractionData)
                .build()
                .getQuery();
            try (QueryExecutor executor = queryExecutorFactory.provide(query)) {
                BlobOutputWriter writer = blobOutputFactory.provide(extractionData);
                Extractor extractor = extractorFactory.provide(extractionData.getType());
                ResultSet resultSet = executor.execute();
                if (resultSet.isBeforeFirst()) {
                    extractor.apply(resultSet, writer.outputStream());
                    blobService.setLastUpdated(extractionData.getContainer());
                }

                log.info("Completed processing data for caseType {} with prefix {}", extractionData.getContainer(), extractionData.getPrefix());
            } catch (Exception e) {
                log.error("Error processing case {}", extractionData.getContainer(), e);
            }
        }
    }
}
