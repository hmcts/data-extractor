package uk.gov.hmcts.reform.dataextractor.config;

import lombok.Builder;
import lombok.Getter;
import org.springframework.boot.context.properties.ConstructorBinding;
import org.springframework.util.StringUtils;

import uk.gov.hmcts.reform.dataextractor.model.Output;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

@ConstructorBinding
@Getter
@Builder
public class ExtractionData {

    static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd");

    static final String QUERY_ORDER = "ORDER BY CE.created_date ASC;";
    private String container;
    private Output type;
    private String caseType;
    private String prefix;
    private String date;

    public ExtractionData(String container, Output type, String caseType, String prefix, String date) {
        this.container = container;
        this.type = type;
        this.prefix = prefix;
        this.date = StringUtils.isEmpty(date)  ? getDefaultStartDate() : date;
        this.caseType = caseType;
    }

    public String getFileName() {
        return String.format("%s-%s.%s", prefix, date, type.getExtension());
    }

    private String getDefaultStartDate() {
        LocalDate localDate = LocalDate.now().minusDays(1);
        return DATE_TIME_FORMATTER.format(localDate);
    }
}
