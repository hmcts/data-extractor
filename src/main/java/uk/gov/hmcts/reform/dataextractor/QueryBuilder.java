package uk.gov.hmcts.reform.dataextractor;

import lombok.Builder;
import lombok.Value;

import uk.gov.hmcts.reform.dataextractor.config.ExtractionData;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Optional;

@Value
@Builder
public class QueryBuilder {
    static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd");
    static final String BASE_QUERY = "SELECT to_char(current_timestamp, 'YYYYMMDD-HH24MI') AS extraction_date,\n"
        + "CE.id           AS case_metadata_event_id,\n"
        + "CE.case_data_id     AS ce_case_data_id,\n"
        + "CE.created_date     AS ce_created_date,\n"
        + "trim(CE.case_type_id)   AS ce_case_type_id,\n"
        + "CE.case_type_version    AS ce_case_type_version,\n"
        + "CE.state_id    AS ce_state_id,\n"
        + "CE.data AS ce_data\n"
        + "FROM case_event CE\n"
        + "WHERE CE.case_type_id = '%s'\n"
        + "AND CE.security_classification = 'PUBLIC'\n";

    static final String QUERY_ORDER = "ORDER BY CE.created_date ASC;";

    private final ExtractionData extractionData;

    private LocalDate fromDate;

    private LocalDate toDate;

    public QueryBuilder(ExtractionData extractionData, LocalDate fromDate, LocalDate toDate) {
        this.extractionData = extractionData;
        this.fromDate = fromDate;
        this.toDate = toDate;
    }

    public QueryBuilder(ExtractionData extractionData, LocalDate fromDate) {
        this.extractionData = extractionData;
        this.fromDate = fromDate;
        this.toDate = LocalDate.now();
    }

    public String getQuery() {
        StringBuilder query = new StringBuilder(String.format(BASE_QUERY, extractionData.getCaseType()));

        String fromDateQuery = Optional.ofNullable(fromDate)
            .map(DATE_TIME_FORMATTER::format)
            .map(date -> String.format("AND CE.created_date >= (to_date('%s', 'yyyyMMdd') + time '00:00') ", date))
            .orElse("");
        query.append(String.format("AND CE.created_date <= (to_date('%s', 'yyyyMMdd') + time '00:00') ", DATE_TIME_FORMATTER.format(toDate)))
            .append(fromDateQuery)
            .append(QUERY_ORDER);
        return query.toString();
    }
}
