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

    private static final String REBUILD_QUERY = "SELECT current_timestamp AS extraction_date,\n"
        + "  ce.case_data_id AS ce_case_data_id\n"
        + ", cd.created_date AS cd_created_date\n"
        + ", cd.last_modified AS cd_last_modified\n"
        + ", cd.jurisdiction AS cd_jurisdiction\n"
        + ", cd.state AS cd_latest_state\n"
        + ", cd.reference AS cd_reference\n"
        + ", cd.security_classification AS cd_security_classification\n"
        + ", cd.version AS cd_version\n"
        + ", cd.last_state_modified_date AS cd_last_state_modified_date\n"
        + ", ce.id AS ce_id\n"
        + ", ce.created_date AS ce_created_date\n"
        + ", ce.event_id AS ce_event_id\n"
        + ", ce.summary AS ce_summary\n"
        + ", ce.description AS ce_description\n"
        + ", ce.user_id AS ce_user_id\n"
        + ", ce.case_type_id AS ce_case_type_id\n"
        + ", ce.case_type_version AS ce_case_type_version\n"
        + ", ce.state_id AS ce_state_id\n"
        + ", ce.data AS ce_data\n"
        + ", ce.user_first_name AS ce_user_first_name\n"
        + ", ce.user_last_name AS ce_user_last_name\n"
        + ", ce.event_name AS ce_event_name\n"
        + ", ce.state_name AS ce_state_name\n"
        + ", ce.data_classification AS ce_data_classification\n"
        + ", ce.security_classification AS ce_security_classification\n"
        + "FROM case_data cd\n"
        + "JOIN case_event ce\n"
        + "ON ce.case_data_id = cd.id "
        + "WHERE ce.case_type_id = '%s'\n";

    static final String QUERY_ORDER = "ORDER BY ce.created_date ASC;";

    private final ExtractionData extractionData;

    private LocalDate fromDate;

    private LocalDate toDate;

    public QueryBuilder(ExtractionData extractionData, LocalDate fromDate, LocalDate toDate) {
        this.extractionData = extractionData;
        this.fromDate = fromDate;
        this.toDate = toDate;
    }

    public String getQuery() {
        StringBuilder query = new StringBuilder(String.format(REBUILD_QUERY, extractionData.getCaseType()));

        String fromDateQuery = Optional.ofNullable(fromDate)
            .map(DATE_TIME_FORMATTER::format)
            .map(date -> String.format("AND ce.created_date >= (to_date('%s', 'yyyyMMdd') + time '00:00') ", date))
            .orElse("");

        query.append(String.format("AND ce.created_date <= (to_date('%s', 'yyyyMMdd') + time '00:00') ", DATE_TIME_FORMATTER.format(toDate)))
            .append(fromDateQuery)
            .append(QUERY_ORDER);
        return query.toString();
    }
}
