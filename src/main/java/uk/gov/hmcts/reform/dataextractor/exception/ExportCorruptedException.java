package uk.gov.hmcts.reform.dataextractor.exception;

public class ExportCorruptedException extends Exception {

    public static final long serialVersionUID = 123458L;

    public ExportCorruptedException(String message) {
        super(message);
    }
}
