package uk.gov.hmcts.reform.dataextractor.task;

import com.azure.core.http.rest.PagedIterable;
import com.azure.storage.blob.models.BlobContainerItem;
import com.azure.storage.blob.models.BlobItem;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import uk.gov.hmcts.reform.dataextractor.model.Output;
import uk.gov.hmcts.reform.dataextractor.service.impl.BlobServiceImpl;

@Component
@Slf4j
@RequiredArgsConstructor
public class ValidateBlobTask implements PreExecutor {

    private final BlobServiceImpl blobService;

    private final  @Value("${task.validateBlob:false}") boolean enabled;

    @Override
    public void execute() {
        if (isEnabled()) {
            log.info("Executing ValidateBlobTask");
            for (BlobContainerItem containerItem : blobService.listContainers()) {
                PagedIterable<BlobItem> blobs = blobService.listContainerBlobs(containerItem.getName());
                blobs.stream()
                    .forEach(blobItem -> {
                        if (blobService.validateBlob(containerItem.getName(), blobItem.getName(), Output.JSON_LINES)) {
                            log.info("Blob in {} with name {} is correct", containerItem.getName(), blobItem.getName());
                        } else {
                            log.warn("Blob in {} with name {} not valid", containerItem.getName(), blobItem.getName());
                        }

                    });
            }
            log.info("ValidateBlobTask completed");
        }
    }

    @Override
    public boolean isEnabled() {
        return enabled;
    }
}
