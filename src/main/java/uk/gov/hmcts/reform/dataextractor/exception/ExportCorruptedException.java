package uk.gov.hmcts.reform.dataextractor.exception;

public class ExportCorruptedException extends Exception {
    public ExportCorruptedException(String message) {
        super(message);
    }
}
