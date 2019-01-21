package com.gpelkie.connector.library;

import com.datastax.driver.core.Session;

/**
 * Created by gpelkie on 1/21/19.
 */
public class BookRepository {

    static final String TABLE_NAME = "books";
    private final Session session;
    private final String keyspace;

    public BookRepository(Session session, String keyspace) {
        this.session = session;
        this.keyspace = keyspace;
    }

    /**
     * Create a books table.
     */
    public void createTable() {
        StringBuilder sb = new StringBuilder("CREATE TABLE IF NOT EXISTS ")
                .append(keyspace).append(".")
                .append(TABLE_NAME).append("(")
                .append("id uuid PRIMARY KEY, ")
                .append("title text,")
                .append("subject text);");

        String query = sb.toString();
        session.execute(query);
    }

    /**
     * Add a column to the books table.
     *
     * @param columnName column to add
     * @param columnType column type
     */
    public void appendColumn(String columnName, String columnType) {
        StringBuilder sb = new StringBuilder("ALTER TABLE ")
                .append(keyspace).append(".")
                .append(TABLE_NAME).append(" ADD ")
                .append(columnName).append(" ")
                .append(columnType).append(";");

        String query = sb.toString();
        session.execute(query);
    }
}
