package uk.gov.hmcts.reform.dataextractor.service.impl;

import com.azure.storage.blob.ProgressReceiver;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BlobProgressReceiver implements ProgressReceiver {
    @Override
    public void reportProgress(long bytesTransferred) {
        log.debug("Current uploaded bytes " + bytesTransferred);
    }
}
