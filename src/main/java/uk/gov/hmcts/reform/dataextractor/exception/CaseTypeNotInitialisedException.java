package uk.gov.hmcts.reform.dataextractor.exception;

public class CaseTypeNotInitialisedException extends  Exception {

    public static final long serialVersionUID = 123456L;

    public CaseTypeNotInitialisedException(String message) {
        super(message);
    }
}
