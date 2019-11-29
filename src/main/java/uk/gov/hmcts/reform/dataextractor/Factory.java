package uk.gov.hmcts.reform.dataextractor;

@FunctionalInterface
public interface Factory<T, R> {
    R provide(T input);
}
