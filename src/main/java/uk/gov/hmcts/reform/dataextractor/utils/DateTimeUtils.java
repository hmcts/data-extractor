package uk.gov.hmcts.reform.dataextractor.utils;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

public final class DateTimeUtils {

    static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd");

    static final DateTimeFormatter CCD_DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");


    public static String dateToString(LocalDate date) {
        return DATE_FORMATTER.format(date);
    }

    public static LocalDate stringToDate(String date) {
        return LocalDate.parse(date, CCD_DATE_FORMATTER);
    }

    private DateTimeUtils() {

    }
}
