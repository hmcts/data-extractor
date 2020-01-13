package uk.gov.hmcts.reform.dataextractor.config;

import org.junit.jupiter.api.Test;
import uk.gov.hmcts.reform.dataextractor.DataExtractorApplication;

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
        LocalDate localDate = LocalDate.now().minusDays(1);
        String expectedDate = ExtractionData.DATE_TIME_FORMATTER.format(localDate);

        ExtractionData extractionData = ExtractionData
            .builder()
            .caseType(caseType)
            .build();
        String extractionQuery = extractionData.getQuery();

        String expectedCondition1 = "case_type_id = 'DIVORCE'";
        String expectedCondition2 = String.format("created_date >= (to_date('%s', 'yyyyMMdd') + time '00:00'", expectedDate);
        String expectedCondition3 = String.format("created_date <= (to_date('%s', 'yyyyMMdd') + time '23:59')", expectedDate);

        assertTrue(extractionQuery.contains(expectedCondition1));
        assertTrue(extractionQuery.contains(expectedCondition2));
        assertTrue(extractionQuery.contains(expectedCondition3));
    }

    @Test
    public void testQueryIsOverriddenRightFilter() {
        String caseType = "DIVORCE";
        String expectedQuery = "Select * from caseData";
        ExtractionData extractionData = ExtractionData
            .builder()
            .caseType(caseType)
            .query(expectedQuery)
            .build();
        String extractionQuery = extractionData.getQuery();
        assertThat(extractionQuery, is(expectedQuery));
    }

    @Test
    public void testDateIsOverriddenRightFilter() {
        String caseType = "DIVORCE";
        String expectedDate = "20100102";

        ExtractionData extractionData = ExtractionData
            .builder()
            .caseType(caseType)
            .date(expectedDate)
            .build();
        String extractionQuery = extractionData.getQuery();

        String expectedCondition1 = String.format("created_date >= (to_date('%s', 'yyyyMMdd') + time '00:00'", expectedDate);
        String expectedCondition2 = String.format("created_date <= (to_date('%s', 'yyyyMMdd') + time '23:59')", expectedDate);
        assertTrue(extractionQuery.contains(expectedCondition1));
        assertTrue(extractionQuery.contains(expectedCondition2));
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
            .type(DataExtractorApplication.Output.JSON_LINES)
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
            .type(DataExtractorApplication.Output.JSON_LINES)
            .build();
        String fileName = extractionData.getFileName();
        assertThat(expectedFileName, is(fileName));
    }
}
