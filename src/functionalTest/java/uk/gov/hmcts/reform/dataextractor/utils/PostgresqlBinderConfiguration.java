package uk.gov.hmcts.reform.dataextractor.utils;

import com.ninja_squad.dbsetup.bind.Binder;
import com.ninja_squad.dbsetup.bind.DefaultBinderConfiguration;

import java.sql.ParameterMetaData;
import java.sql.SQLException;
import java.sql.Types;

// See: https://github.com/Ninja-Squad/DbSetup/issues/55
public class PostgresqlBinderConfiguration extends DefaultBinderConfiguration {

    /**
     * Adds support for Postgresql type placeholders when the column metadata
     * indicates that the {@link java.sql.Types} is OTHER (e.g. a custom type or
     * jsonb) See also {@link PostgresqlCustomTypeBinder} for implementation details
     */
    @Override
    public Binder getBinder(ParameterMetaData metadata, int param) throws SQLException {

        if (metadata.getParameterType(param) == Types.OTHER) {
            return new PostgresqlCustomTypeBinder();
        }

        return super.getBinder(metadata, param);
    }

}