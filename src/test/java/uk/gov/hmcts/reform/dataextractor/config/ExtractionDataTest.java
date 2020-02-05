package uk.gov.hmcts.reform.dataextractor.config;

import org.junit.jupiter.api.Test;

import uk.gov.hmcts.reform.dataextractor.QueryBuilder;
import uk.gov.hmcts.reform.dataextractor.model.Output;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ExtractionDataTest {
    static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd");

    @Test
    public void testQueryHasRightFilter() {
        String caseType = "DIVORCE";
        LocalDate fromDate = LocalDate.now().minusDays(10);
        ExtractionData extractionData = ExtractionData
            .builder()
            .caseType(caseType)
            .build();
        String expectedToDate = ExtractionData.DATE_TIME_FORMATTER.format(LocalDate.now());
        String expectedFromDate = ExtractionData.DATE_TIME_FORMATTER.format(fromDate);

        String extractionQuery = QueryBuilder
            .builder()
            .fromDate(fromDate)
            .extractionData(extractionData)
            .build()
            .getQuery();

        String expectedCondition1 = "case_type_id = 'DIVORCE'";
        String expectedCondition2 = String.format("created_date >= (to_date('%s', 'yyyyMMdd') + time '00:00'", expectedFromDate);
        String expectedCondition3 = String.format("created_date <= (to_date('%s', 'yyyyMMdd') + time '00:00')", expectedToDate);

        assertTrue(extractionQuery.contains(expectedCondition1));
        assertTrue(extractionQuery.contains(expectedCondition2));
        assertTrue(extractionQuery.contains(expectedCondition3));
    }

    @Test
    public void testDateIsOverriddenGetValidFileName() {
        String caseType = "Test";
        String expectedDate = "20100102";
        String expectedFileName = "Test-20100102.jsonl";
        ExtractionData extractionData = ExtractionData
            .builder()
            .prefix("Test")
            .caseType(caseType)
            .date(expectedDate)
            .type(Output.JSON_LINES)
            .build();
        String fileName = extractionData.getFileName();
        assertThat(expectedFileName, is(fileName));
    }

    @Test
    public void testWithDefaultDateGetValidFileName() {
        String caseType = "Test";
        String expectedDate = DATE_TIME_FORMATTER.format(LocalDateTime.now().minusDays(1));
        String expectedFileName = String.format("Test-%s.jsonl", expectedDate);
        ExtractionData extractionData = ExtractionData
            .builder()
            .prefix("Test")
            .caseType(caseType)
            .type(Output.JSON_LINES)
            .build();
        String fileName = extractionData.getFileName();
        assertThat(expectedFileName, is(fileName));
    }
}
