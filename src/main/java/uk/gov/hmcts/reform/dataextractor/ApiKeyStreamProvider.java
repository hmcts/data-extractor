package uk.gov.hmcts.reform.dataextractor;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.specialized.BlobOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.OutputStream;

public class ApiKeyStreamProvider implements OutputStreamProvider {

    private static final Logger LOGGER = LoggerFactory.getLogger(ApiKeyStreamProvider.class);

    private final String connectStr;

    public ApiKeyStreamProvider(String connectStr) {
        this.connectStr = connectStr;
    }

    private BlobServiceClient getBlobServiceClient () {
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
            LOGGER.info("Created container {}" ,  containerName);
        }
        BlobClient blob = container.getBlobClient(fileName);

        BlobOutputStream blobOutputStream ;
        if (!blob.exists()) {
            blobOutputStream = blob.getBlockBlobClient().getBlobOutputStream();

        } else {
            blobOutputStream =  blob.getAppendBlobClient().getBlobOutputStream();
        }

        return blobOutputStream;
    }
}
