package uk.gov.hmcts.reform.dataextractor.utils;

import org.junit.jupiter.api.Test;

import uk.gov.hmcts.reform.dataextractor.config.ExtractionData;
import uk.gov.hmcts.reform.dataextractor.model.Output;

import java.time.LocalDate;

import static org.junit.jupiter.api.Assertions.assertEquals;

class BlobFileUtilsTest {

    @Test
    void testGetFileName() {
        assertEquals("Test-20001001.jsonl",
            BlobFileUtils.getFileName(ExtractionData
                    .builder()
                    .prefix("Test")
                    .type(Output.JSON_LINES)
                    .build(),
                LocalDate
                    .now()
                    .withYear(2000)
                    .withMonth(10)
                    .withDayOfMonth(1)),
            "Expected Name");

    }
}