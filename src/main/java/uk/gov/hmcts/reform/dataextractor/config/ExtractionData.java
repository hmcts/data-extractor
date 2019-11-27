package uk.gov.hmcts.reform.dataextractor.config;

import lombok.Builder;
import lombok.Value;
import org.springframework.boot.context.properties.ConstructorBinding;
import uk.gov.hmcts.reform.dataextractor.DataExtractorApplication;

@ConstructorBinding
@Value
@Builder
public class ExtractionData {
    private String container;
    private DataExtractorApplication.Output type;
    private String query;
    private String prefix;
}
