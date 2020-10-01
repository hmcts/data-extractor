package uk.gov.hmcts.reform.dataextractor.test.utils.logger;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;

public class TestAppender extends AppenderBase<ILoggingEvent> {

    private TestRecorder recorder;

    public TestAppender() {
        this.setName("Test appender");
        this.start();
    }

    @Override
    protected void append(ILoggingEvent o) {
        if (recorder != null) {
            recorder.append(o.toString());
        }
    }

    public void setRecorder(TestRecorder recorder) {
        this.recorder = recorder;
    }

    public void clean() {
        this.recorder = null;
    }
}
