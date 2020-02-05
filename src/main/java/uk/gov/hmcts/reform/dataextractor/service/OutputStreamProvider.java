package uk.gov.hmcts.reform.dataextractor.service;

import uk.gov.hmcts.reform.dataextractor.model.Output;

import java.io.OutputStream;

public interface OutputStreamProvider {

    OutputStream getOutputStream(String containerName, String fileName, Output outputType);

}
