package uk.gov.hmcts.reform.dataextractor.utils;

import uk.gov.hmcts.reform.dataextractor.config.ExtractionData;

import java.time.LocalDate;

public final class BlobFileUtils {

    public static String getFileName(ExtractionData data, LocalDate date) {
        if (data == null || date == null) {
            throw new IllegalArgumentException("Null input not accepted");
        }
        return String.format("%s-%s.%s",  data.getPrefix(), DateTimeUtils.dateToString(date), data.getType().getExtension());
    }

    private BlobFileUtils() {

    }
}
