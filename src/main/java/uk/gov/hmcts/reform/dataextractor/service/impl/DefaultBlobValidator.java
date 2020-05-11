package uk.gov.hmcts.reform.dataextractor.service.impl;

import org.springframework.stereotype.Component;

import uk.gov.hmcts.reform.dataextractor.service.BlobOutputValidator;

@Component
public class DefaultBlobValidator implements BlobOutputValidator {

    public boolean isValid(String input) {
        return  true;
    }

    public boolean isNotValid(String input) {
        return !isValid(input);
    }
}
