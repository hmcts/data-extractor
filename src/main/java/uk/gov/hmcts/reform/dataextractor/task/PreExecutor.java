package uk.gov.hmcts.reform.dataextractor.task;

public interface PreExecutor {

    void execute();

    boolean isEnabled();

}
