package uk.gov.hmcts.reform.dataextractor.task;

import com.azure.core.http.rest.PagedIterable;
import com.azure.storage.blob.models.BlobContainerItem;
import com.azure.storage.blob.models.BlobItem;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import uk.gov.hmcts.reform.dataextractor.service.impl.BlobServiceImpl;

import java.time.OffsetDateTime;
import java.util.Optional;

@Component
@Slf4j
public class SetLastUpdateMetadataTask implements PreExecutor {

    @Autowired
    private BlobServiceImpl blobService;

    @Value("${task.SetLastUpdateMetadata:false}")
    private boolean enabled;

    @Override
    public void execute() {
        for (BlobContainerItem containerItem: blobService.listContainers()) {
            PagedIterable<BlobItem> blobs = blobService.listContainerBlobs(containerItem.getName());
            Optional<OffsetDateTime> lastUpdate = blobs.stream()
                .map(blobItem -> blobItem.getProperties().getLastModified())
                .max(OffsetDateTime::compareTo);

            if (lastUpdate.isPresent()) {
                OffsetDateTime lastUpdatedField = lastUpdate.get();
                blobService.setLastUpdated(containerItem.getName(), lastUpdatedField.toLocalDate());
                log.info("Set last update on container {} date to {}", containerItem.getName(), lastUpdatedField.toLocalDate().toString());
            } else {
                log.info("Container {} has not blobs", containerItem.getName());
            }
        }
    }

    @Override
    public boolean isEnabled() {
        return enabled;
    }

}
