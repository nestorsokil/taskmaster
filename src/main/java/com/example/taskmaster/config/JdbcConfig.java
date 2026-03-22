package com.example.taskmaster.config;

import com.example.taskmaster.domain.Tags;
import org.postgresql.util.PGobject;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.convert.ReadingConverter;
import org.springframework.data.convert.WritingConverter;
import org.springframework.data.jdbc.core.mapping.JdbcValue;
import org.springframework.data.jdbc.repository.config.AbstractJdbcConfiguration;
import org.springframework.lang.NonNull;

import java.sql.Array;
import java.sql.JDBCType;
import java.sql.SQLException;
import java.util.List;

/**
 * Registers a JDBC reading converter so Spring Data JDBC can map
 * PostgreSQL {@code JSONB} ({@link PGobject}) back to {@code String}.
 * The write direction is handled by {@code stringtype=unspecified} in the JDBC URL.
 */
@Configuration
public class JdbcConfig extends AbstractJdbcConfiguration {

    @SuppressWarnings("null")
    @Override
    @NonNull
    public List<Object> userConverters() {
        return List.of(
                new PGobjectToStringConverter(),
                new ArrayToTagsConverter(),
                new StringArrayToTagsConverter(),
                new TagsToJdbcValueConverter()
        );
    }

    @ReadingConverter
    static class PGobjectToStringConverter implements Converter<PGobject, String> {
        @Override
        public String convert(@NonNull PGobject source) {
            return source.getValue();
        }
    }

    @ReadingConverter
    static class ArrayToTagsConverter implements Converter<Array, Tags> {
        @Override
        public Tags convert(@NonNull Array source) {
            try {
                String[] array = (String[]) source.getArray();
                return new Tags(array == null ? List.of() : List.of(array));
            } catch (SQLException e) {
                throw new RuntimeException("Failed to convert SQL array to Tags", e);
            }
        }
    }

    @ReadingConverter
    static class StringArrayToTagsConverter implements Converter<String[], Tags> {
        @Override
        public Tags convert(@NonNull String[] source) {
            return new Tags(List.of(source));
        }
    }

    @WritingConverter
    static class TagsToJdbcValueConverter implements Converter<Tags, JdbcValue> {
        @Override
        public JdbcValue convert(@NonNull Tags source) {
            return JdbcValue.of(source.values().toArray(String[]::new), JDBCType.ARRAY);
        }
    }
}
