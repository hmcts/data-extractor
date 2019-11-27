package uk.gov.hmcts.reform.dataextractor.config;

import lombok.Data;
import uk.gov.hmcts.reform.dataextractor.DataExtractorApplication;

@Data
public class ExtractionData {
    private String container;
    private DataExtractorApplication.Output type;
    private String query;
}
