package uk.gov.hmcts.reform.dataextractor;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import uk.gov.hmcts.reform.dataextractor.config.ApplicationConfig;
import uk.gov.hmcts.reform.dataextractor.model.Output;
import uk.gov.hmcts.reform.dataextractor.service.Extractor;
import uk.gov.hmcts.reform.dataextractor.service.impl.ExtractorCsv;
import uk.gov.hmcts.reform.dataextractor.service.impl.ExtractorJson;
import uk.gov.hmcts.reform.dataextractor.service.impl.ExtractorJsonLines;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class DataExtractorOutputTest {

    private Factory<Output, Extractor> extractorFactory;

    @BeforeEach
    public void setup() {
        ApplicationConfig config = new ApplicationConfig();
        extractorFactory = config.extractorFactory();
    }

    @Test
    public void whenDefaultOutputRequested_thenJsonLinesReturned() {
        assertEquals(Output.JSON_LINES, Output.defaultOutput());
    }

    @Test
    public void whenFromIsCsv_thenCsvOutputIsReturned() {
        assertEquals(Output.CSV, Output.from("Csv"));
    }

    @Test
    public void whenFromIsJson_thenJsonOutputIsReturned() {
        assertEquals(Output.JSON, Output.from("jSon"));
        assertEquals(Output.JSON, Output.from("JSON"));
    }

    @Test
    public void whenFromIsJsonLines_thenJsonLinesOutputIsReturned() {
        assertEquals(Output.JSON_LINES, Output.from("jSonLiNes"));
        assertEquals(Output.JSON_LINES, Output.from("JSONLINES"));
    }

    @Test
    public void whenFromIsNotSpecified_thenJsonLinesOutputIsReturned() {
        assertEquals(Output.JSON_LINES, Output.from(null));
    }

    @Test
    public void whenFromNotValid_thenJsonLinesOutputIsReturned() {
        assertEquals(Output.JSON_LINES, Output.from("notValid"));
    }

    @Test
    public void whenFactoryOutputIsJson_thenExtractorJsonIsReturned() {
        assertEquals(
            ExtractorJson.class,
            extractorFactory.provide(Output.JSON).getClass()
        );
    }

    @Test
    public void whenFactoryOutputIsCsv_thenExtractorCsvIsReturned() {
        assertEquals(
            ExtractorCsv.class,
            extractorFactory.provide(Output.CSV).getClass()
        );
    }

    @Test
    public void whenFactoryOutputIsJsonLines_thenExtractorJsonLinesIsReturned() {
        assertEquals(
            ExtractorJsonLines.class,
            extractorFactory.provide(Output.JSON_LINES).getClass()
        );
    }
}
