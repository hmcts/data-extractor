package uk.gov.hmcts.reform.dataextractor.service.impl;

import org.springframework.stereotype.Component;

import uk.gov.hmcts.reform.dataextractor.service.BlobOutputValidator;

@Component
public class DefaultBlobValidator implements BlobOutputValidator {

    @Override
    public boolean isValid(String input) {
        return true;
    }

    @Override
    public boolean isNotValid(String input) {
        return !isValid(input);
    }
}
