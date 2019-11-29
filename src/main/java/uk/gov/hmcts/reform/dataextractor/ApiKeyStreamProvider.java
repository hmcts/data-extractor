package uk.gov.hmcts.reform.dataextractor;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.specialized.BlockBlobClient;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.io.OutputStream;

@Component
@Slf4j
@ConditionalOnProperty(value = "etl.connection-string")
public class ApiKeyStreamProvider implements OutputStreamProvider {

    private final String connectStr;

    public ApiKeyStreamProvider(@Value("${etl.connection-string}") String connectStr) {
        this.connectStr = connectStr;
    }

    protected BlobServiceClient getBlobServiceClient() {
        try {
            return new BlobServiceClientBuilder().connectionString(connectStr).buildClient();
        } catch (IllegalArgumentException e) {
            throw new WriterException(e);
        }
    }

    @Override
    public OutputStream getOutputStream(String containerName, String fileName, DataExtractorApplication.Output outputType) {

        BlobServiceClient client = getBlobServiceClient();
        BlobContainerClient container;
        container = client.getBlobContainerClient(containerName);
        if (!container.exists()) {
            container.create();
            log.info("Created container {}", containerName);
        }
        BlobClient blob = container.getBlobClient(fileName);
        BlockBlobClient appendClient = blob.getBlockBlobClient();
        return appendClient.getBlobOutputStream();
    }

}
