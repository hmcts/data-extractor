package uk.gov.hmcts.reform.dataextractor;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import uk.gov.hmcts.reform.dataextractor.config.ExtractionData;
import uk.gov.hmcts.reform.dataextractor.config.Extractions;
import uk.gov.hmcts.reform.dataextractor.model.Output;
import uk.gov.hmcts.reform.dataextractor.service.CaseDataService;
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

    @Autowired
    private CaseDataService caseDataService;

    @SuppressWarnings("PMD.CloseResource")
    public void execute() {
        for (ExtractionData extractionData : extractions.getCaseTypes()) {
            log.info("Processing data for caseType {} with prefix {}", extractionData.getContainer(), extractionData.getPrefix());
            LocalDate lastUpdated = blobService.getContainerLastUpdated(extractionData.getContainer());
            int counter = 10; //TODO Fix the name

            if (lastUpdated == null) {
                lastUpdated = caseDataService.getFirstEventDate(extractionData.getCaseType());
            }

            //Batch extraction per months so the first to shard first extraction
            LocalDate toDate = lastUpdated.plusMonths(1).isBefore(LocalDate.now()) ? lastUpdated.plusMonths(1) : LocalDate.now();

            QueryBuilder queryBuilder = QueryBuilder
                .builder()
                .fromDate(lastUpdated)
                .toDate(toDate)
                .extractionData(extractionData)
                .build();

            try (QueryExecutor executor = queryExecutorFactory.provide(queryBuilder.getQuery())) {
                BlobOutputWriter writer = blobOutputFactory.provide(extractionData);
                Extractor extractor = extractorFactory.provide(extractionData.getType());
                ResultSet resultSet = executor.execute();
                if (resultSet.isBeforeFirst()) {
                    extractor.apply(resultSet, writer.outputStream(String.valueOf(counter)));
                    blobService.setLastUpdated(extractionData.getContainer(), queryBuilder.getToDate());
                    counter++;
                }

                log.info("Completed processing data for caseType {} with prefix {} with end date {}",
                    extractionData.getContainer(), extractionData.getPrefix(), queryBuilder.getToDate());
            } catch (Exception e) {
                log.error("Error processing case {}", extractionData.getContainer(), e);
            }
        }

    }
}
