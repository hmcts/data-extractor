package uk.gov.hmcts.reform.dataextractor;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;


public class DataExtractorApplicationTest {

    @Test
    public void whenDefaultOutputRequested_thenJsonLinesReturned() {
        assertEquals(DataExtractorApplication.Output.JSON_LINES, DataExtractorApplication.Output.defaultOutput());
    }

}
