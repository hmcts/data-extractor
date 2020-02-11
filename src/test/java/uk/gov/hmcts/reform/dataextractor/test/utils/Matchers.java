package uk.gov.hmcts.reform.dataextractor.test.utils;

import static org.mockito.ArgumentMatchers.argThat;

public final class Matchers {

    public static <T>  T argsThat(T value) {
        return argThat(new ReflectionEqualMatcher<>(value));
    }

    private Matchers() {

    }
}
