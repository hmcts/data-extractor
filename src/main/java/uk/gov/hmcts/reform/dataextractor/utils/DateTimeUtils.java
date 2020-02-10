package uk.gov.hmcts.reform.dataextractor.utils;

import java.time.LocalDate;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;

public final class DateTimeUtils {

    static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd");
    static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("mmss");


    public static String getCurrentTime() {
        return LocalTime.now().format(TIME_FORMATTER);
    }

    public static String dateToString(LocalDate date) {
        return DATE_FORMATTER.format(date);
    }

    private DateTimeUtils() {

    }
}
