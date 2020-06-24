package uk.gov.hmcts.reform.dataextractor;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import uk.gov.hmcts.reform.dataextractor.config.ExtractionData;
import uk.gov.hmcts.reform.dataextractor.config.Extractions;
import uk.gov.hmcts.reform.dataextractor.exception.CaseTypeNotInitialisedException;
import uk.gov.hmcts.reform.dataextractor.model.CaseDefinition;
import uk.gov.hmcts.reform.dataextractor.model.ExtractionWindow;
import uk.gov.hmcts.reform.dataextractor.model.Output;
import uk.gov.hmcts.reform.dataextractor.service.CaseDataService;
import uk.gov.hmcts.reform.dataextractor.service.Extractor;
import uk.gov.hmcts.reform.dataextractor.service.impl.BlobServiceImpl;
import uk.gov.hmcts.reform.dataextractor.utils.BlobFileUtils;

import java.sql.ResultSet;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
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
    private static final int DEFAULT_EXTRACTION_WINDOW = 7;

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

    @Value("${extraction.max.batch.row:100_000}")
    private int maxRowPerBatch;

    public void execute(boolean initialLoad) {
        LocalDate now = LocalDate.now();
        List<CaseDefinition> caseDefinitions = caseDataService.getCaseDefinitions();
        for (CaseDefinition caseDefinition : caseDefinitions) {
            ExtractionData extractionData = getExtractionData(caseDefinition);
            log.info("Processing data for caseType {} with prefix {}", extractionData.getContainer(), extractionData.getPrefix());
            processData(extractionData, now, initialLoad);
            log.info("Processing data for caseType {} with prefix {} completed", extractionData.getContainer(), extractionData.getPrefix());

        }
    }

    private int calculateExtractionWindow(String caseType) {
        ExtractionWindow dates = caseDataService.getDates(caseType);
        long caseCount = caseDataService.getCaseTypeRows(caseType);
        LocalDateTime dateTime1 = LocalDateTime.ofInstant(Instant.ofEpochMilli(dates.getStart()), ZoneId.systemDefault());
        LocalDateTime dateTime2 = LocalDateTime.ofInstant(Instant.ofEpochMilli(dates.getEnd()), ZoneId.systemDefault());
        long days = ChronoUnit.DAYS.between(dateTime1, dateTime2);
        double portions = Math.ceil(caseCount / maxRowPerBatch);
        return (int) Math.ceil(days / portions);
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
    private void processData(ExtractionData extractionData, LocalDate now, boolean initialLoad) {

        LocalDate lastUpdated = null;
        try {
            lastUpdated = getLastUpdateFromContainer(extractionData, initialLoad);
        } catch (Exception e) {
            log.error("Case type not initialised {}", extractionData.getCaseType());
            return;
        }

        LocalDate toDate;
        QueryExecutor executor = null;
        BlobOutputWriter writer = null;
        Extractor extractor = extractorFactory.provide(extractionData.getType());
        final int window = initialLoad ? calculateExtractionWindow(extractionData.getCaseType()) : DEFAULT_EXTRACTION_WINDOW;
        do {
            long startTime = LocalDateTime.now().toInstant(ZoneOffset.UTC).toEpochMilli();

            try {
                toDate = getToDate(lastUpdated, now, window);
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
                    int extractedRows = applyExtraction(extractor, resultSet, fileName, extractionData, writer, toDate);
                    long endTime = LocalDateTime.now().toInstant(ZoneOffset.UTC).toEpochMilli();
                    // log.info("Total data processed in current batch: {} for case type {} ", extractedRows, extractionData.getCaseType());
                    log.info("Batch completed in {} ms extracting {} with number rows extracted {}",
                        endTime - startTime, extractionData.getCaseType(), extractedRows);
                } else {
                    toDate = getToDate(lastUpdated, now, window);
                    log.info("There is no records for caseType {}", extractionData.getContainer());
                }
                lastUpdated = toDate;


            } catch (Exception e) {
                log.error("Error processing case {}", extractionData.getContainer(), e);
                break;
            } finally {
                closeQueryExecutor(executor);
                executor = null;
            }
        } while (toDate.isBefore(now));
    }

    private int applyExtraction(Extractor extractor, ResultSet resultSet, String fileName, ExtractionData extractionData,
                                BlobOutputWriter writer, LocalDate toDate) {
        int extractedRows = extractor.apply(resultSet, writer.outputStream(fileName));
        if (!blobService.validateBlob(extractionData.getContainer(), fileName, extractionData.getType())) {
            blobService.deleteBlob(extractionData.getContainer(), fileName);
            log.warn("Corrupted blob {}  has been deleted", fileName);
        } else {
            blobService.setLastUpdated(extractionData.getContainer(), toDate);
        }
        return extractedRows;
    }

    private void closeQueryExecutor(QueryExecutor queryExecutor) {
        if (queryExecutor != null) {
            queryExecutor.close();
        }
    }

    private LocalDate getToDate(LocalDate lastUpdated, LocalDate currentDate, int window) {
        return lastUpdated.plusDays(window).isBefore(currentDate) ? lastUpdated.plusDays(window) : currentDate;
    }

    private Map<String, ExtractionData> getCaseTypesToExtractMap() {
        return extractions.getCaseTypes().stream()
            .filter(extractionData -> !extractionData.isDisabled())
            .collect(Collectors.toMap(ExtractionData::getCaseType, Function.identity()));
    }

    private LocalDate getLastUpdateFromContainer(ExtractionData extractionData, boolean initialLoad) throws CaseTypeNotInitialisedException {
        LocalDate lastUpdated = blobService.getContainerLastUpdated(extractionData.getContainer());
        if (lastUpdated == null) {
            if (initialLoad) {
                return caseDataService.getFirstEventDate(extractionData.getCaseType());
            } else {
                throw new CaseTypeNotInitialisedException(String.format("Case type %s not initialised", extractionData.getCaseType()));
            }
        }

        return lastUpdated;

    }

    //    private LocalDate getLastUpdateDate(ExtractionData extractionData) {
    //        blobService.getContainerLastUpdated(extractionData.getContainer());
    //        return Optional
    //            .ofNullable(blobService.getContainerLastUpdated(extractionData.getContainer()))
    //            .orElseGet(() -> caseDataService.getFirstEventDate(extractionData.getCaseType()));
    //    }
}
