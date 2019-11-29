package uk.gov.hmcts.reform.dataextractor.config;

import lombok.Builder;
import lombok.Value;
import org.springframework.boot.context.properties.ConstructorBinding;
import uk.gov.hmcts.reform.dataextractor.DataExtractorApplication;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

import static java.time.ZoneOffset.UTC;

@ConstructorBinding
@Value
@Builder
public class ExtractionData {
    static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd");

    private String container;
    private DataExtractorApplication.Output type;
    private String query;
    private String prefix;
    private String date;

    public String getFileName() {
        return String.format("%s-%s.%s", prefix,
            date == null ? DATE_TIME_FORMATTER.format(LocalDateTime.now(ZoneId.from(UTC))) : date,
            type.getExtension());
    }
}
