package uk.gov.hmcts.reform.dataextractor.utils;

import com.ninja_squad.dbsetup.bind.Binder;
import org.postgresql.util.PGobject;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Allows the use of custom types in Postgresql.
 *
 * <p>Append the type to the value using the ::objectType notation to
 * attempt to set the {@link PGobject} type appropriately
 *
 * <p>For example, given a table FOO with a column VAL of type jsonb
 *
 * <pre>
 * Insert ins = Insert.into("FOO")
 *                    .columns("VAL")
 *                    .values("{\"somekey\": \"somevalue\"}"::jsonb")
 *                    .build();
 * </pre>
 * See: https://github.com/Ninja-Squad/DbSetup/issues/55
 */
public class PostgresqlCustomTypeBinder implements Binder {

    private final Pattern objectTypePattern = Pattern.compile("(.*?)::(.*?)");

    @Override
    public void bind(PreparedStatement statement, int paramIndex, Object value) throws SQLException {

        Matcher m = objectTypePattern.matcher(value.toString());

        if (!m.matches()) {
            throw new SQLException(
                "Unable to determine custom type - expected value::objectType syntax, value was: " + value);
        }

        String bareValue = m.group(1);
        String type = m.group(2);

        PGobject obj = new PGobject();
        obj.setType(type);
        obj.setValue(bareValue);

        statement.setObject(paramIndex, obj);
    }

}