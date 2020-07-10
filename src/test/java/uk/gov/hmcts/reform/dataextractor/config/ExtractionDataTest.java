package uk.gov.hmcts.reform.dataextractor.config;

import org.junit.jupiter.api.Test;

import uk.gov.hmcts.reform.dataextractor.QueryBuilder;
import uk.gov.hmcts.reform.dataextractor.model.Output;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ExtractionDataTest {
    static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd");

    @Test
    void testQueryHasRightFilter() {
        String caseType = "DIVORCE";
        LocalDate fromDate = LocalDate.now().minusDays(10);
        ExtractionData extractionData = ExtractionData
            .builder()
            .container(caseType)
            .caseType(caseType)
            .build();
        String expectedToDate = DATE_FORMATTER.format(LocalDate.now());
        String expectedFromDate = DATE_FORMATTER.format(fromDate);

        String extractionQuery = QueryBuilder
            .builder()
            .fromDate(fromDate)
            .toDate(LocalDate.now())
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
    void testWithDefaultDateGetValidFileName() {
        String caseType = "Test";
        String expectedFileName = "Test.jsonl";
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
