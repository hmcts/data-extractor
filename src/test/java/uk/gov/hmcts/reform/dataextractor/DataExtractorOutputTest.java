package uk.gov.hmcts.reform.dataextractor;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import uk.gov.hmcts.reform.dataextractor.config.ApplicationConfig;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class DataExtractorOutputTest {

    private Factory<DataExtractorApplication.Output, Extractor> extractorFactory;

    @BeforeEach
    public void setup() {
        ApplicationConfig config = new ApplicationConfig();
        extractorFactory = config.extractorFactory();
    }

    @Test
    public void whenDefaultOutputRequested_thenJsonLinesReturned() {
        assertEquals(DataExtractorApplication.Output.JSON_LINES, DataExtractorApplication.Output.defaultOutput());
    }

    @Test
    public void whenFromIsCsv_thenCsvOutputIsReturned() {
        assertEquals(DataExtractorApplication.Output.CSV, DataExtractorApplication.Output.from("Csv"));
    }

    @Test
    public void whenFromIsJson_thenJsonOutputIsReturned() {
        assertEquals(DataExtractorApplication.Output.JSON, DataExtractorApplication.Output.from("jSon"));
        assertEquals(DataExtractorApplication.Output.JSON, DataExtractorApplication.Output.from("JSON"));
    }

    @Test
    public void whenFromIsJsonLines_thenJsonLinesOutputIsReturned() {
        assertEquals(DataExtractorApplication.Output.JSON_LINES, DataExtractorApplication.Output.from("jSonLiNes"));
        assertEquals(DataExtractorApplication.Output.JSON_LINES, DataExtractorApplication.Output.from("JSONLINES"));
    }

    @Test
    public void whenFromIsNotSpecified_thenJsonLinesOutputIsReturned() {
        assertEquals(DataExtractorApplication.Output.JSON_LINES, DataExtractorApplication.Output.from(null));
    }

    @Test
    public void whenFactoryOutputIsJson_thenExtractorJsonIsReturned() {
        assertEquals(
            ExtractorJson.class,
            extractorFactory.provide(DataExtractorApplication.Output.JSON).getClass()
        );
    }

    @Test
    public void whenFactoryOutputIsCsv_thenExtractorCsvIsReturned() {
        assertEquals(
            ExtractorCsv.class,
            extractorFactory.provide(DataExtractorApplication.Output.CSV).getClass()
        );
    }

    @Test
    public void whenFactoryOutputIsJsonLines_thenExtractorJsonLinesIsReturned() {
        assertEquals(
            ExtractorJsonLines.class,
            extractorFactory.provide(DataExtractorApplication.Output.JSON_LINES).getClass()
        );
    }
}
