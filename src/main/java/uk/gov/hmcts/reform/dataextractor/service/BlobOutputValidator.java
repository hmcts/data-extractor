package uk.gov.hmcts.reform.dataextractor.service;

public interface BlobOutputValidator {

    boolean isValid(String input);

    boolean isNotValid(String input);
}
