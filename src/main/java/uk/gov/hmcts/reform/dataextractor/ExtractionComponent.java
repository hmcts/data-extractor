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
@SuppressWarnings({"PMD.UnusedLocalVariable", "PMD.UnusedPrivateField"})
public class ExtractionComponent {

    private static final Output DEFAULT_OUTPUT = Output.JSON_LINES;

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

    public void execute() {
        LocalDate now = LocalDate.now();

        //List<CaseType> caseTypes = caseDataService.getCaseTypes(null, null);
        for (ExtractionData extractionData : getCaseTypesToExtract()) {
            //        for (CaseType caseType : caseTypes) {
            //            ExtractionData extractionData =  ExtractionData.builder()
            //                .container(String.format("ccd-%s-%s",caseType.getJurisdiction(),
            //                    caseType.getCaseType()).toLowerCase().replace('_', '-'))
            //                .caseType(caseType.getCaseType())
            //                .type(DEFAULT_OUTPUT)
            //                .build();

            log.info("Processing data for caseType {} with prefix {}", extractionData.getContainer(), extractionData.getPrefix());
            exec(extractionData, now);
        }

    }

    private void exec(ExtractionData extractionData, LocalDate now) {
        LocalDate toDate;
        QueryExecutor executor = null;
        BlobOutputWriter writer = null;
        Extractor extractor = extractorFactory.provide(extractionData.getType());
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
                log.info("Query to execute : {}", queryBuilder.getQuery());
                executor = queryExecutorFactory.provide(queryBuilder.getQuery());
                writer = blobOutputFactory.provide(extractionData);
                @SuppressWarnings("PMD.CloseResource")
                ResultSet resultSet = executor.execute();
                String fileName = BlobFileUtils.getFileName(extractionData, toDate);

                if (resultSet.isBeforeFirst()) {
                    applyExtracgtion(extractor, resultSet, fileName, extractionData, writer, toDate, queryBuilder);
                } else {
                    toDate = now;
                    log.info("There is no records for caseType {}", extractionData.getContainer());
                }

            } catch (Exception e) {
                log.error("Error processing case {}", extractionData.getContainer(), e);
                break;
            } finally {
                closeQueryExecutor(executor);
                executor = null;
            }
        } while (toDate.isBefore(now));
    }

    private void applyExtraction(Extractor extractor, ResultSet resultSet, String fileName, ExtractionData extractionData,
                                 BlobOutputWriter writer, LocalDate toDate, QueryBuilder queryBuilder) {
        extractor.apply(resultSet, writer.outputStream(fileName));
        if (!blobService.validateBlob(extractionData.getContainer(), fileName, extractionData.getType())) {
            blobService.deleteBlob(extractionData.getContainer(), fileName);
            log.warn("Corrupted blob {}  has been deleted", fileName);
        } else {
            blobService.setLastUpdated(extractionData.getContainer(), toDate);
        }

        log.info("Completed processing data for caseType {} with prefix {} with end date {}",
            extractionData.getContainer(), extractionData.getPrefix(), queryBuilder.getToDate());
    }

    private void closeQueryExecutor(QueryExecutor queryExecutor) {
        if (queryExecutor != null) {
            queryExecutor.close();
        }
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
