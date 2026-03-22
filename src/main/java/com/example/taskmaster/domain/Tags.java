package com.example.taskmaster.domain;

import java.util.List;

/**
 * Wrapper for a list of string tags, used as a column type in Spring Data JDBC entities.
 *
 * <p>Spring Data JDBC maps {@code List<String>} as a one-to-many relationship.
 * This wrapper type, paired with custom read/write converters in {@code JdbcConfig},
 * maps directly to a PostgreSQL {@code TEXT[]} column instead.
 */
public record Tags(List<String> values) {

    public static final Tags EMPTY = new Tags(List.of());

    public Tags {
        if (values == null) values = List.of();
    }
}
