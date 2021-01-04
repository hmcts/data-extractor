package uk.gov.hmcts.reform.dataextractor.config;

import lombok.Builder;
import lombok.Getter;
import lombok.ToString;
import org.springframework.boot.context.properties.ConstructorBinding;

import uk.gov.hmcts.reform.dataextractor.model.Output;

import java.util.Locale;

@ConstructorBinding
@Getter
@Builder
@ToString
public class ExtractionData {

    private String container;
    private Output type;
    private String caseType;
    private String prefix;
    private boolean disabled;

    public ExtractionData(String container, Output type, String caseType, String prefix, boolean disabled) {
        this.container = container == null ? null : container.toLowerCase(Locale.UK);
        this.type = type;
        this.prefix = prefix;
        this.caseType = caseType;
        this.disabled = disabled;
    }

    public String getFileName() {
        return String.format("%s.%s", prefix,  type.getExtension());
    }

}
