package uk.gov.hmcts.reform.dataextractor.service.impl;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(MockitoExtension.class)
class DefaultBlobValidatorTest {

    @InjectMocks
    private DefaultBlobValidator classToTest;


    @Test
    void testIsValid() {
        assertTrue(classToTest.isValid("anyString"));
    }

    @Test
    void testNotValid() {
        assertFalse(classToTest.isNotValid("anyString"));
    }

    @Test
    void testNotValidOnNullValue() {
        assertFalse(classToTest.isNotValid(null));
    }
}