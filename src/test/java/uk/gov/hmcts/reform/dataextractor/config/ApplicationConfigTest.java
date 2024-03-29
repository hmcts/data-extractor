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
class ApplicationConfigTest {

    static final String SQL_QUERY = "sqlQuery";

    @Mock
    private OutputStreamProvider outputStreamProvider;

    @Mock
    private JsonValidator jsonValidator;

    @Mock
    private DefaultBlobValidator defaultBlobValidator;

    @Spy
    private DbConfig dbConfig;

    @Mock
    private Output mockOutput;

    @InjectMocks
    private ApplicationConfig classToTest;

    @SuppressWarnings("PMD.JUnitAssertionsShouldIncludeMessage")
    @Test
    void givenBlobOutputFactory_thenCreateBlobOutputWriter() {
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
        try (BlobOutputWriter result = blobOutput.provide(extractionData);
             BlobOutputWriter expected = new BlobOutputWriter(containerName, extractionData.getFileName(), outputType, outputStreamProvider)) {
            assertThat(result).isEqualToIgnoringGivenFields(expected, "outputStream");
        }
    }

    @Test
    void givenBlobOutputFactory_thenCreateQueryExecutor() {
        dbConfig.setBaseDir("BaseDir");
        dbConfig.setPassword("password");
        dbConfig.setUrl("ulr");
        dbConfig.setUser("user");
        Factory<String, QueryExecutor> queryExecutorFactory = classToTest.queryExecutorFactory();

        try (QueryExecutor result = queryExecutorFactory.provide("sqlQuery");
            QueryExecutor expected = new QueryExecutor(dbConfig.getUrl(), dbConfig.getUser(), dbConfig.getPassword(), "sqlQuery")) {
            assertThat(result).isEqualToComparingFieldByField(expected);
        }
    }

    @Test
    void testJsonValidator() {
        assertThat(classToTest.blobOutputValidator().provide(Output.JSON_LINES)).isEqualTo(jsonValidator);
    }

    @Test
    void testDefaultValidator() {

        assertThat(classToTest.blobOutputValidator().provide(mockOutput)).isEqualTo(defaultBlobValidator);
    }
}
