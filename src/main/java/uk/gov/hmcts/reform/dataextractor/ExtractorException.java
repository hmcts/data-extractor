package uk.gov.hmcts.reform.dataextractor;

public class ExtractorException extends RuntimeException {

    public ExtractorException(String message) {
        super(message);
    }

    public ExtractorException(Throwable cause) {
        super(cause);
    }

}
