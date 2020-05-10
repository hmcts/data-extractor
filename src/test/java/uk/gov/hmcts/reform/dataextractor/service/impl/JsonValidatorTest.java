package uk.gov.hmcts.reform.dataextractor.service.impl;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class JsonValidatorTest {

    private static final String VALID_JSON = "{\"key\": true}";
    private static final String INVALID_JSON = "{\"key\": true";


    @InjectMocks
    private JsonValidator classToTest;

    @Spy
    private ObjectMapper objectMapper;

    @Mock
    private ObjectReader objectReader;

    @Test
    void isValid() {
        assertFalse(classToTest.isValid(INVALID_JSON));
    }

    @Test
    void givenInvalidJson_whenIsNotValid_returnTrue() {
        assertTrue(classToTest.isNotValid(INVALID_JSON));
    }

    @Test
    void givenValidJson_whenIsNotValid_returnFalse() {
        assertFalse(classToTest.isNotValid(VALID_JSON));
    }

    @Test
    void givenJsonException_whenIsNotValid_returnTrue() throws JsonProcessingException {
        when(objectMapper.reader()).thenReturn(objectReader);
        when(objectReader.readTree(VALID_JSON)).thenThrow(new JsonGenerationException(new Exception(), null));
        assertTrue(classToTest.isNotValid(VALID_JSON));
    }
}