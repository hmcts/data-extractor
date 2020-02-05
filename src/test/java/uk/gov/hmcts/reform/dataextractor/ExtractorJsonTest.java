package uk.gov.hmcts.reform.dataextractor;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import uk.gov.hmcts.reform.dataextractor.exception.ExtractorException;
import uk.gov.hmcts.reform.dataextractor.service.impl.ExtractorJson;

import java.io.OutputStream;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class ExtractorJsonTest {

    @InjectMocks
    public ExtractorJson classToTest;

    @Mock
    private ResultSet resultSet;

    @Mock
    private OutputStream outputStream;

    @Test
    public void givenSqlException_thenPropagateException() throws SQLException {
        when(resultSet.getMetaData()).thenThrow(new SQLException());
        assertThrows(ExtractorException.class, () -> classToTest.apply(resultSet, outputStream));
    }
}
