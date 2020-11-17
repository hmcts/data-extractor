package uk.gov.hmcts.reform.dataextractor;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import uk.gov.hmcts.reform.dataextractor.config.ApplicationConfig;
import uk.gov.hmcts.reform.dataextractor.model.Output;
import uk.gov.hmcts.reform.dataextractor.service.Extractor;
import uk.gov.hmcts.reform.dataextractor.service.impl.ExtractorCsv;
import uk.gov.hmcts.reform.dataextractor.service.impl.ExtractorJson;
import uk.gov.hmcts.reform.dataextractor.service.impl.ExtractorJsonLines;

import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(MockitoExtension.class)
class DataExtractorOutputTest {

    private Factory<Output, Extractor> extractorFactory;

    @Mock
    private ExtractorCsv extractorCsv;
    @Mock
    private ExtractorJson extractorJson;
    @Mock
    private ExtractorJsonLines extractorJsonLines;

    @InjectMocks
    private ApplicationConfig config;

    @BeforeEach
    void setup() {
        extractorFactory = config.extractorFactory();
    }

    @Test
    void whenDefaultOutputRequested_thenJsonLinesReturned() {
        assertEquals(Output.JSON_LINES, Output.defaultOutput());
    }

    @Test
    void whenFromIsCsv_thenCsvOutputIsReturned() {
        assertEquals(Output.CSV, Output.from("Csv"));
    }

    @Test
    void whenFromIsJson_thenJsonOutputIsReturned() {
        assertEquals(Output.JSON, Output.from("jSon"));
        assertEquals(Output.JSON, Output.from("JSON"));
    }

    @Test
    void whenFromIsJsonLines_thenJsonLinesOutputIsReturned() {
        assertEquals(Output.JSON_LINES, Output.from("jSonLiNes"));
        assertEquals(Output.JSON_LINES, Output.from("JSONLINES"));
    }

    @Test
    void whenFromIsNotSpecified_thenJsonLinesOutputIsReturned() {
        assertEquals(Output.JSON_LINES, Output.from(null));
    }

    @Test
    void whenFromNotValid_thenJsonLinesOutputIsReturned() {
        assertEquals(Output.JSON_LINES, Output.from("notValid"));
    }

    @Test
    void whenFactoryOutputIsJson_thenExtractorJsonIsReturned() {
        assertEquals(
            extractorJson,
            extractorFactory.provide(Output.JSON)
        );
    }

    @Test
    void whenFactoryOutputIsCsv_thenExtractorCsvIsReturned() {
        assertEquals(
            extractorCsv,
            extractorFactory.provide(Output.CSV)
        );
    }

    @Test
    void whenFactoryOutputIsJsonLines_thenExtractorJsonLinesIsReturned() {
        assertEquals(
            extractorJsonLines,
            extractorFactory.provide(Output.JSON_LINES)
        );
    }
}
