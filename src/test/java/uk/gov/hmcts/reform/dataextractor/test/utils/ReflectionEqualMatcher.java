package uk.gov.hmcts.reform.dataextractor.test.utils;

import org.mockito.ArgumentMatcher;
import org.mockito.internal.matchers.apachecommons.ReflectionEquals;

public class ReflectionEqualMatcher<T> implements ArgumentMatcher<T> {

    private final T value;

    public ReflectionEqualMatcher(T value) {
        this.value = value;
    }

    @Override
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public boolean matches(T argument) {
        if (Boolean.logicalOr(argument == null, value == null)) {
            return   value == argument;
        }
        ReflectionEquals details = new ReflectionEquals(value);
        return details.matches(argument);
    }

}