package uk.gov.hmcts.reform.dataextractor.functional.postdeploy;

import uk.gov.hmcts.reform.dataextractor.model.Output;

import java.time.format.DateTimeFormatter;

public final class TestConstants {

    public static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd");
    public static final String BLOB_PREFIX = "CCD-TEST";
    public static final String TEST_CONTAINER = "test-container";
    public static final Output OUTPUT_TYPE = Output.JSON_LINES;
    public static final String BREAK_LINE = "\r\n";
    public static final int DATA_DAYS = 7;
    public static final String CASE_TYPE = "Caveat";

    private TestConstants() {

    }
}
