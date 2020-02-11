package uk.gov.hmcts.reform.dataextractor.functional.postdeploy;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import uk.gov.hmcts.reform.dataextractor.exception.WriterException;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;

@Component
public class BlobReader {

    private final String connectStr;

    public BlobReader(@Value("${etl.connection-string}") String connectStr) {
        this.connectStr = connectStr;
    }

    public BlobServiceClient getBlobServiceClient() {
        try {
            return new BlobServiceClientBuilder().connectionString(connectStr).buildClient();
        } catch (IllegalArgumentException e) {
            throw new WriterException(e);
        }
    }

    public String  readFile(String container, String fileName) {
        OutputStream output = new ByteArrayOutputStream();

        BlobServiceClient blobContainer = getBlobServiceClient();
        BlobClient blobClient = blobContainer.getBlobContainerClient(container).getBlobClient(fileName);
        blobClient.download(output);
        return output.toString();
    }

    public BlobClient  getBlobClient(String container, String fileName) {
        BlobServiceClient blobContainer = getBlobServiceClient();
        return blobContainer.getBlobContainerClient(container).getBlobClient(fileName);
    }
}
