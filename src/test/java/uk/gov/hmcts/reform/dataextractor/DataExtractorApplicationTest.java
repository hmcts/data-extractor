package uk.gov.hmcts.reform.dataextractor;

import com.typesafe.config.ConfigFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;


public class DataExtractorApplicationTest {

    @Test
    public void whenDefaultOutputRequested_thenJsonLinesReturned() {
        assertEquals(DataExtractorApplication.Output.JSON_LINES, DataExtractorApplication.Output.defaultOutput());
    }

    @Test
    public void whenApplicationCreated_thenConfigurationRead(@TempDir Path tempDir) throws Exception {
        ConfigFactory.invalidateCaches();
        System.setProperty("config.resource", "application-alt.conf");
        DataExtractorApplication application = new DataExtractorApplication();
        assertNotNull(application.getConfig());
        assertEquals(application.getConfig().etlDbUser, "user");
        assertEquals(application.getConfig().etlDbPassword, "password");
    }

    @Test
    public void whenFromIsCsv_thenCsvOutputIsReturned() {
        assertEquals(DataExtractorApplication.Output.CSV, DataExtractorApplication.Output.from("Csv"));
    }

    @Test
    public void whenFromIsJson_thenJsonOutputIsReturned() {
        assertEquals(DataExtractorApplication.Output.JSON, DataExtractorApplication.Output.from("jSon"));
        assertEquals(DataExtractorApplication.Output.JSON, DataExtractorApplication.Output.from("JSON"));
    }

    @Test
    public void whenFromIsJsonLines_thenJsonLinesOutputIsReturned() {
        assertEquals(DataExtractorApplication.Output.JSON_LINES, DataExtractorApplication.Output.from("jSonLiNes"));
        assertEquals(DataExtractorApplication.Output.JSON_LINES, DataExtractorApplication.Output.from("JSONLINES"));
    }

    @Test
    public void whenFromIsNotSpecified_thenJsonLinesOutputIsReturned() {
        assertEquals(DataExtractorApplication.Output.JSON_LINES, DataExtractorApplication.Output.from(null));
    }

    @Test
    public void whenFactoryOutputIsJson_thenExtractorJsonIsReturned() {
        assertEquals(
            ExtractorJson.class,
            DataExtractorApplication.extractorFactory(DataExtractorApplication.Output.JSON).getClass()
        );
    }

    @Test
    public void whenFactoryOutputIsCsv_thenExtractorCsvIsReturned() {
        assertEquals(
            ExtractorCsv.class,
            DataExtractorApplication.extractorFactory(DataExtractorApplication.Output.CSV).getClass()
        );
    }

    @Test
    public void whenFactoryOutputIsJsonLines_thenExtractorJsonLinesIsReturned() {
        assertEquals(
            ExtractorJsonLines.class,
            DataExtractorApplication.extractorFactory(DataExtractorApplication.Output.JSON_LINES).getClass()
        );
    }

    @Test
    public void whenDbUserAndPasswordAreFiles_thenFilesAreRead(@TempDir Path tempDir) throws Exception {
        Files.write(tempDir.resolve("user-file"), "username".getBytes());
        Files.write(tempDir.resolve("password-file"), "password1\npassword2".getBytes());
        String baseDir = tempDir.normalize().toString();
        DataExtractorApplication application = new DataExtractorApplication(baseDir);
        assertEquals(application.getConfig().etlDbUser, "username");
        assertEquals(application.getConfig().etlDbPassword, "password1");
    }

}
