package uk.gov.hmcts.reform.dataextractor.utils;

import lombok.extern.slf4j.Slf4j;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;


@Slf4j
public class LoggedBufferedOutputStream  extends BufferedOutputStream {
    public LoggedBufferedOutputStream(OutputStream out) {
        super(out);
    }

    public LoggedBufferedOutputStream(OutputStream out, int size) {
        super(out, size);
    }

    @Override
    public void close() throws IOException {
        super.close();
        log.info("closed logged output stream");
    }
}
