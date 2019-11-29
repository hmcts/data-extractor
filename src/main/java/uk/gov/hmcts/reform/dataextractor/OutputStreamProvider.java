package uk.gov.hmcts.reform.dataextractor;

import java.io.OutputStream;

public interface OutputStreamProvider {

    OutputStream getOutputStream(String containerName, String fileName, DataExtractorApplication.Output outputType);

}
