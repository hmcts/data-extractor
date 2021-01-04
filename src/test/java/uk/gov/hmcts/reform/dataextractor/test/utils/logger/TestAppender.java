package uk.gov.hmcts.reform.dataextractor.test.utils.logger;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;

public class TestAppender extends AppenderBase<ILoggingEvent> {

    private TestRecorder recorder;

    public TestAppender() {
        super();
        this.setName("Test appender");
        this.start();
    }

    @Override
    protected void append(ILoggingEvent eventObject) {
        if (recorder != null) {
            recorder.append(eventObject.toString());
        }
    }

    public void setRecorder(TestRecorder recorder) {
        this.recorder = recorder;
    }
}
