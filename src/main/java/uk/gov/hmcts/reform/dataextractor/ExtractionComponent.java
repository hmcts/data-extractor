package uk.gov.hmcts.reform.dataextractor;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import uk.gov.hmcts.reform.dataextractor.config.ExtractionData;
import uk.gov.hmcts.reform.dataextractor.config.Extractions;
import uk.gov.hmcts.reform.dataextractor.model.CaseDefinition;
import uk.gov.hmcts.reform.dataextractor.model.Output;
import uk.gov.hmcts.reform.dataextractor.service.CaseDataService;
import uk.gov.hmcts.reform.dataextractor.service.Extractor;
import uk.gov.hmcts.reform.dataextractor.service.impl.BlobServiceImpl;
import uk.gov.hmcts.reform.dataextractor.utils.BlobFileUtils;

import java.sql.ResultSet;
import java.time.LocalDate;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

@Component
@Slf4j
public class ExtractionComponent {

    private static final Output DEFAULT_OUTPUT = Output.JSON_LINES;
    private static final Locale GB_LOCALE = Locale.ENGLISH;

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
        List<CaseDefinition> caseDefinitions = caseDataService.getCaseDefinitions();
        for (CaseDefinition caseDefinition : caseDefinitions) {
            ExtractionData extractionData = getExtractionData(caseDefinition);
            log.info("Processing data for caseType {} with prefix {}", extractionData.getContainer(), extractionData.getPrefix());
            processData(extractionData, now);
        }
    }

    private ExtractionData getExtractionData(CaseDefinition caseDefinition) {
        Map<String, ExtractionData> extractionConfig = getCaseTypesToExtractMap();
        return extractionConfig.getOrDefault(caseDefinition.getCaseType(), ExtractionData.builder()
            .container(String.format("%s-%s", caseDefinition.getJurisdiction(), caseDefinition.getCaseType())
                .toLowerCase(GB_LOCALE).replace('_', '-'))
            .caseType(caseDefinition.getCaseType())
            .prefix(String.format("ccd-%s-%s", caseDefinition.getJurisdiction(), caseDefinition.getCaseType())
                .toLowerCase(GB_LOCALE).replace('_', '-'))
            .type(DEFAULT_OUTPUT)
            .build());
    }

    @SuppressWarnings("PMD.CloseResource")
    private void processData(ExtractionData extractionData, LocalDate now) {
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
                ResultSet resultSet = executor.execute();

                if (resultSet.isBeforeFirst()) {
                    String fileName = BlobFileUtils.getFileName(extractionData, toDate);
                    applyExtraction(extractor, resultSet, fileName, extractionData, writer, toDate, queryBuilder);
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

    private Map<String, ExtractionData> getCaseTypesToExtractMap() {
        return extractions.getCaseTypes().stream()
            .filter(extractionData -> !extractionData.isDisabled())
            .collect(Collectors.toMap(ExtractionData::getCaseType, Function.identity()));
    }

    private LocalDate getLastUpdateDate(ExtractionData extractionData) {
        blobService.getContainerLastUpdated(extractionData.getContainer());
        return Optional
            .ofNullable(blobService.getContainerLastUpdated(extractionData.getContainer()))
            .orElseGet(() -> caseDataService.getFirstEventDate(extractionData.getCaseType()));
    }
}
