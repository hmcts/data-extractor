package uk.gov.hmcts.reform.dataextractor.utils;

import org.junit.jupiter.api.Test;

import java.time.LocalDate;

import static org.junit.jupiter.api.Assertions.assertEquals;


class DateTimeUtilsTest {

    @Test
    void parseRightDate() {
        LocalDate localDate = LocalDate.now().withYear(2000).withMonth(1).withDayOfMonth(1);
        assertEquals("20000101", DateTimeUtils.dateToString(localDate), "Valid date parser");
    }
}
