package uk.gov.hmcts.reform.dataextractor.config;

import org.junit.jupiter.api.Test;

import java.time.LocalDate;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ExtractionDataTest {

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
}
