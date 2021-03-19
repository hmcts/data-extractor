package uk.gov.hmcts.reform.dataextractor;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import uk.gov.hmcts.reform.dataextractor.config.ExtractionData;
import uk.gov.hmcts.reform.dataextractor.config.Extractions;
import uk.gov.hmcts.reform.dataextractor.exception.ExportCorruptedException;
import uk.gov.hmcts.reform.dataextractor.model.CaseDefinition;
import uk.gov.hmcts.reform.dataextractor.model.Output;
import uk.gov.hmcts.reform.dataextractor.service.CaseDataService;
import uk.gov.hmcts.reform.dataextractor.service.Extractor;
import uk.gov.hmcts.reform.dataextractor.service.impl.BlobServiceImpl;
import uk.gov.hmcts.reform.dataextractor.utils.BlobFileUtils;
import uk.gov.hmcts.reform.mi.micore.utils.DateTimeUtils;

import java.sql.ResultSet;
import java.time.Clock;
import java.time.LocalDate;
import java.util.List;
import java.util.Locale;
import java.util.Map;
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

    @Autowired
    private Clock clock;

    @Value("${extraction.toDate:}")
    private String limitDate;

    public void execute(boolean initialLoad) {

        LocalDate now = getEndDate(initialLoad);
        List<CaseDefinition> caseDefinitions = caseDataService.getCaseDefinitions();
        log.info("Total case definitions loaded {}", caseDefinitions.size());
        for (CaseDefinition caseDefinition : caseDefinitions) {
            ExtractionData extractionData = getExtractionData(caseDefinition);
            log.info(extractionData.toString());
            log.info("Processing data for caseType {} , container {} with prefix {}", extractionData.getCaseType(),
                extractionData.getContainer(), extractionData.getPrefix());
            processData(extractionData, now, initialLoad);
            log.info("Processing data for caseType {} , container {} with prefix {} completed",
                extractionData.getCaseType(), extractionData.getContainer(), extractionData.getPrefix());

        }
    }

    private LocalDate getEndDate(boolean initialLoad) {
        if (initialLoad && StringUtils.isNotBlank(limitDate)) {
            return DateTimeUtils.stringToLocalDate(limitDate);
        } else {
            return LocalDate.now(clock);
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

    @SuppressWarnings({"PMD.CloseResource","PMD.NullAssignment"})
    private void processData(ExtractionData extractionData, LocalDate executionTime, boolean initialLoad) {

        LocalDate lastUpdated = null;
        try {
            lastUpdated = getLastUpdateFromContainer(extractionData);
        } catch (Exception e) {
            log.warn("Case type not initialised {}", extractionData.getCaseType());
            return;
        }

        LocalDate toDate = null;
        QueryExecutor executor = null;
        Extractor extractor = extractorFactory.provide(extractionData.getType());
        final int extractionWindow = caseDataService.calculateExtractionWindow(extractionData.getCaseType(),
            lastUpdated, executionTime, initialLoad);
        do {
            long executionStartTime = System.currentTimeMillis();

            try {
                toDate = getExtractionToDate(lastUpdated, executionTime, extractionWindow);
                QueryBuilder queryBuilder = QueryBuilder
                    .builder()
                    .fromDate(lastUpdated)
                    .toDate(toDate)
                    .extractionData(extractionData)
                    .build();
                log.info("Query to execute : {}", queryBuilder.getQuery());
                executor = queryExecutorFactory.provide(queryBuilder.getQuery());
                BlobOutputWriter writer = blobOutputFactory.provide(extractionData);
                ResultSet resultSet = executor.execute();

                if (resultSet.isBeforeFirst()) {
                    String fileName = BlobFileUtils.getFileName(extractionData, toDate);
                    int extractedRows = applyExtraction(extractor, resultSet, fileName, extractionData, writer, toDate);

                    long batchExecutionEndTime = System.currentTimeMillis();
                    log.info("Batch completed in {} ms extracting {} with number rows extracted {}",
                        batchExecutionEndTime - executionStartTime, extractionData.getCaseType(), extractedRows);
                } else {
                    toDate = getExtractionToDate(lastUpdated, executionTime, extractionWindow);
                    log.info("There is no records for caseType {}", extractionData.getContainer());
                }
                lastUpdated = toDate;
            } catch (ExportCorruptedException corruptedFile) {
                log.error("Corrupted file has been discarded", corruptedFile);
            } catch (Exception e) {
                log.error("Error processing case {}", extractionData.getContainer(), e);
                break;
            } finally {
                closeQueryExecutor(executor);
                executor = null;
            }
        } while (toDate.isBefore(executionTime));
    }

    private int applyExtraction(Extractor extractor, ResultSet resultSet, String fileName, ExtractionData extractionData,
                                BlobOutputWriter writer, LocalDate toDate) throws ExportCorruptedException {
        int extractedRows = extractor.apply(resultSet, writer.getOutputStream(fileName));
        if (blobService.validateBlob(extractionData.getContainer(), fileName, extractionData.getType())) {
            blobService.setLastUpdated(extractionData.getContainer(), toDate);
            return extractedRows;
        } else {
            blobService.deleteBlob(extractionData.getContainer(), fileName);
            log.warn("Corrupted blob {}  has been deleted", fileName);
            throw new ExportCorruptedException("File corrupted");
        }
    }

    private void closeQueryExecutor(QueryExecutor queryExecutor) {
        if (queryExecutor != null) {
            queryExecutor.close();
        }
    }

    private LocalDate getExtractionToDate(LocalDate lastUpdated, LocalDate currentDate, int window) {
        return lastUpdated.plusDays(window).isBefore(currentDate) ? lastUpdated.plusDays(window) : currentDate;
    }

    private Map<String, ExtractionData> getCaseTypesToExtractMap() {
        return extractions.getCaseTypes().stream()
            .filter(extractionData -> !extractionData.isDisabled())
            .collect(Collectors.toMap(ExtractionData::getCaseType, Function.identity()));
    }

    private LocalDate getLastUpdateFromContainer(ExtractionData extractionData) {
        LocalDate lastUpdated = blobService.getContainerLastUpdated(extractionData.getContainer());
        if (lastUpdated == null) {
            return caseDataService.getFirstEventDate(extractionData.getCaseType());
        }

        return lastUpdated;

    }
}
