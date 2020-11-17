package uk.gov.hmcts.reform.dataextractor.service.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import uk.gov.hmcts.reform.dataextractor.service.BlobOutputValidator;

@Component
public class JsonValidator implements BlobOutputValidator {

    @Autowired
    private ObjectMapper jsonMapper;

    public boolean isValid(String input) {
        try {
            jsonMapper.reader().readTree(input);
            return true;
        } catch (JsonProcessingException e) {
            return false;
        }
    }

    public boolean isNotValid(String input) {
        return !isValid(input);
    }
}
