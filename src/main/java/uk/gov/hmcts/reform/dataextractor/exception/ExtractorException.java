package uk.gov.hmcts.reform.dataextractor.exception;

public class ExtractorException extends RuntimeException {

    public static final long serialVersionUID = 123459L;

    public ExtractorException(Throwable cause) {
        super(cause);
    }

    public ExtractorException(String message) {
        super(message);
    }
}
