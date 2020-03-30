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
import uk.gov.hmcts.reform.dataextractor.utils.BlobFileUtils;

import java.sql.ResultSet;
import java.time.LocalDate;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@Component
@Slf4j
public class ExtractionComponent {

    @Autowired
    private Factory<ExtractionData, BlobOutputWriter> blobOutputFactory;

    @Autowired
    private Factory<String, QueryExecutor> queryExecutorFactory;

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
        LocalDate now = LocalDate.now();

        for (ExtractionData extractionData : getCaseTypesToExtract()) {
            log.info("Processing data for caseType {} with prefix {}", extractionData.getContainer(), extractionData.getPrefix());
            LocalDate toDate = null;
            QueryExecutor executor = null;
            BlobOutputWriter writer = null;

            do {
                try {
                    LocalDate lastUpdated = getLastUpdateDate(extractionData);
                    toDate = getToDate(lastUpdated, now);
                    QueryBuilder queryBuilder = QueryBuilder
                        .builder()
                        .fromDate(lastUpdated)
                        .toDate(toDate)
                        .extractionData(extractionData)
                        .build();
                    executor = queryExecutorFactory.provide(queryBuilder.getQuery());
                    writer = blobOutputFactory.provide(extractionData);
                    Extractor extractor = extractorFactory.provide(extractionData.getType());
                    ResultSet resultSet = executor.execute();
                    if (resultSet.isBeforeFirst()) {
                        extractor.apply(resultSet, writer.outputStream(BlobFileUtils.getFileName(extractionData, toDate)));
                    } else {
                        toDate = now;
                    }
                    blobService.setLastUpdated(extractionData.getContainer(), toDate);

                    log.info("Completed processing data for caseType {} with prefix {} with end date {}",
                        extractionData.getContainer(), extractionData.getPrefix(), queryBuilder.getToDate());

                } catch (Exception e) {
                    log.error("Error processing case {}", extractionData.getContainer(), e);
                    break;
                } finally {
                    closeQueryExecutor(executor);
                    executor = null;
                }
            } while (isToDateBeforeNow(toDate, now));
        }

    }

    private void closeQueryExecutor(QueryExecutor queryExecutor) {
        if (queryExecutor != null) {
            queryExecutor.close();
        }
    }

    private boolean isToDateBeforeNow(LocalDate toDate, LocalDate now) {
        return toDate != null && toDate.isBefore(now);
    }

    private LocalDate getToDate(LocalDate lastUpdated, LocalDate currentDate) {
        return lastUpdated.plusMonths(1).isBefore(currentDate) ? lastUpdated.plusMonths(1) : currentDate;
    }

    private List<ExtractionData> getCaseTypesToExtract() {
        return extractions.getCaseTypes().stream()
            .filter(extractionData -> !extractionData.isDisabled())
            .collect(Collectors.toList());
    }

    private LocalDate getLastUpdateDate(ExtractionData extractionData) {
        return Optional
            .ofNullable(blobService.getContainerLastUpdated(extractionData.getContainer()))
            .orElseGet(() -> caseDataService.getFirstEventDate(extractionData.getCaseType()));
    }
}
