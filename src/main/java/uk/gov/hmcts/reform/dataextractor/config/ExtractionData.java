package uk.gov.hmcts.reform.dataextractor.config;

import lombok.Builder;
import lombok.Getter;
import org.springframework.boot.context.properties.ConstructorBinding;
import org.springframework.util.StringUtils;
import uk.gov.hmcts.reform.dataextractor.DataExtractorApplication;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

@ConstructorBinding
@Getter
@Builder
public class ExtractionData {
    static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd");

    private String container;
    private DataExtractorApplication.Output type;
    private String query;
    private String caseType;
    private String prefix;
    private String date;


    public ExtractionData(String container, DataExtractorApplication.Output type, String query, String caseType, String prefix, String date) {
        this.container = container;
        this.type = type;
        this.prefix = prefix;
        this.date = StringUtils.isEmpty(date)  ? getDefaultStartDate() : date;
        this.caseType = caseType;
        this.query =  StringUtils.isEmpty(query) ? defaultQuery(caseType, this.date) : query;
    }

    public String getFileName() {
        return String.format("%s-%s.%s", prefix, date, type.getExtension());
    }

    private String getDefaultStartDate() {
        LocalDate localDate = LocalDate.now().minusDays(1);
        return DATE_TIME_FORMATTER.format(localDate);
    }

    private String defaultQuery(String caseType, String date) {
        return String.format("SELECT to_char(current_timestamp, 'YYYYMMDD-HH24MI') AS extraction_date,\n"
            + "CE.id           AS case_metadata_event_id,\n"
            + "CE.case_data_id     AS ce_case_data_id,\n"
            + "CE.created_date     AS ce_created_date,\n"
            + "trim(CE.case_type_id)   AS ce_case_type_id,\n"
            + "CE.case_type_version    AS ce_case_type_version,\n"
            + "CE.state_id    AS ce_state_id,\n"
            + "CE.data AS ce_data\n"
            + "FROM case_event CE\n"
            + "WHERE CE.case_type_id = '%s'\n"
            + "AND CE.created_date >= (to_date('%s', 'yyyyMMdd') + time '00:00')\n"
            + "AND CE.created_date <= (to_date('%s', 'yyyyMMdd') + time '23:59')\n"
            + "AND CE.security_classification = 'PUBLIC'\n"
            + "ORDER BY CE.created_date ASC;", caseType, date, date);
    }
}
