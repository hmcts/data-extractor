package uk.gov.hmcts.reform.dataextractor.config;

import lombok.Builder;
import lombok.Getter;
import org.springframework.boot.context.properties.ConstructorBinding;

import uk.gov.hmcts.reform.dataextractor.model.Output;

@ConstructorBinding
@Getter
@Builder
public class ExtractionData {

    private String container;
    private Output type;
    private String caseType;
    private String prefix;

    public ExtractionData(String container, Output type, String caseType, String prefix) {
        this.container = container;
        this.type = type;
        this.prefix = prefix;
        this.caseType = caseType;
    }

    public String getFileName() {
        return String.format("%s.%s", prefix,  type.getExtension());
    }

}
