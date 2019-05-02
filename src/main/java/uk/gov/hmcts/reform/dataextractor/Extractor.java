package uk.gov.hmcts.reform.dataextractor;

import java.io.OutputStream;
import java.sql.ResultSet;

// A BiConsumer ... basically
public interface Extractor {

    void apply(ResultSet resultSet, OutputStream outputStream);

}
