package uk.gov.hmcts.reform.dataextractor;

import org.junit.jupiter.api.Test;

import java.io.OutputStream;

public class ApiKeyStreamProviderTest {

    private ApiKeyStreamProvider  classToTest ;

//    @Test
    public void test() {
        String containerName = "container";
        String fileName= "fileName";
        classToTest = new ApiKeyStreamProvider("test");
        OutputStream stream = classToTest.getOutputStream(containerName, fileName, DataExtractorApplication.Output.JSON_LINES);
    }

}
