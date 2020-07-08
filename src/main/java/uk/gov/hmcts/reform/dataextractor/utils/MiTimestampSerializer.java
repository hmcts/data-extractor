package uk.gov.hmcts.reform.dataextractor.utils;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.sql.Timestamp;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

@Component
public class MiTimestampSerializer extends StdSerializer<Timestamp> {

    public MiTimestampSerializer() {
        super(Timestamp.class);
    }

    @Override
    public void serialize(Timestamp value, JsonGenerator gen, SerializerProvider provider) throws IOException {
        gen.writeString(value.toInstant().atOffset(ZoneOffset.UTC).format(DateTimeFormatter.ISO_DATE_TIME));
    }
}
