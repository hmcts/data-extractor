package uk.gov.hmcts.reform.dataextractor.model;

import java.util.Locale;

public enum Output {
    CSV("csv", "text/csv"),
    JSON("json", "application/json"),
    JSON_LINES("jsonl", "application/x-ndjson");
    // See: https://github.com/wardi/jsonlines/issues/9

    private String extension;
    private String applicationContent;

    Output(String extension, String applicationContent) {
        this.extension = extension;
        this.applicationContent = applicationContent;
    }

    public String getExtension() {
        return extension;
    }

    public String getApplicationContent() {
        return applicationContent;
    }

    public static Output defaultOutput() {
        return Output.JSON_LINES;
    }

    public static Output from(String val) {
        if (val == null) {
            return defaultOutput();
        }
        String normalisedVal = val
            .toLowerCase(Locale.ENGLISH)
            .replaceAll("[-_\\p{Space}]", "");
        if ("csv".equals(normalisedVal)) {
            return Output.CSV;
        } else if ("jsonlines".equals(normalisedVal)) {
            return Output.JSON_LINES;
        } else if ("json".equals(normalisedVal)) {
            return Output.JSON;
        } else {
            return defaultOutput();
        }
    }
}
