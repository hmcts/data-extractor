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
    private boolean disabled;

    public ExtractionData(String container, Output type, String caseType, String prefix, boolean disabled) {
        this.container = container;
        this.type = type;
        this.prefix = prefix;
        this.caseType = caseType;
        this.disabled = disabled;
    }

    public String getFileName() {
        return String.format("%s.%s", prefix,  type.getExtension());
    }

}
