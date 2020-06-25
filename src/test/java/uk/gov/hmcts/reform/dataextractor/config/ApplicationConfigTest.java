package uk.gov.hmcts.reform.dataextractor.config;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

import uk.gov.hmcts.reform.dataextractor.BlobOutputWriter;
import uk.gov.hmcts.reform.dataextractor.Factory;
import uk.gov.hmcts.reform.dataextractor.QueryExecutor;
import uk.gov.hmcts.reform.dataextractor.model.Output;
import uk.gov.hmcts.reform.dataextractor.service.OutputStreamProvider;
import uk.gov.hmcts.reform.dataextractor.service.impl.DefaultBlobValidator;
import uk.gov.hmcts.reform.dataextractor.service.impl.JsonValidator;

import static org.assertj.core.api.Assertions.assertThat;


@ExtendWith(MockitoExtension.class)
public class ApplicationConfigTest {

    @Mock
    private OutputStreamProvider outputStreamProvider;

    @Mock
    private JsonValidator jsonValidator;

    @Mock
    private DefaultBlobValidator defaultBlobValidator;

    @Spy
    private DbConfig dbConfig;

    @InjectMocks
    private ApplicationConfig classToTest;

    @Test
    public void givenBlobOutputFactory_thenCreateBlobOutputWriter() {
        Factory<ExtractionData, BlobOutputWriter> blobOutput = classToTest.blobOutputFactory();
        String containerName = "container";
        String prefix = "testPrefix";
        Output outputType = Output.CSV;
        ExtractionData extractionData = ExtractionData
            .builder()
            .type(outputType)
            .prefix(prefix)
            .container(containerName)
            .build();
        BlobOutputWriter result = blobOutput.provide(extractionData);
        BlobOutputWriter expected = new BlobOutputWriter(containerName, extractionData.getFileName(), outputType, outputStreamProvider);

        assertThat(result).isEqualToIgnoringGivenFields(expected, "outputStream");
    }

    @Test
    public void givenBlobOutputFactory_thenCreateQueryExecutor() {
        dbConfig.setBaseDir("BaseDir");
        dbConfig.setPassword("password");
        dbConfig.setUrl("ulr");
        dbConfig.setUser("user");
        Factory<String, QueryExecutor> queryExecutorFactory = classToTest.queryExecutorFactory();
        QueryExecutor result = queryExecutorFactory.provide("sqlQuery");
        QueryExecutor expected = new QueryExecutor(dbConfig.getUrl(), dbConfig.getUser(), dbConfig.getPassword(), "sqlQuery");
        assertThat(result).isEqualToComparingFieldByField(expected);
    }

    @Test
    public void testJsonValidator() {
        assertThat(classToTest.blobOutputValidator().provide(Output.JSON_LINES)).isEqualTo(jsonValidator);
    }

    @Test
    public void testDefaultValidator() {
        assertThat(classToTest.blobOutputValidator().provide(Output.CSV)).isEqualTo(defaultBlobValidator);
    }
}
