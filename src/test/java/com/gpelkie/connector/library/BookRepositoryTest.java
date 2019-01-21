package com.gpelkie.connector.library;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ResultSet;
import com.gpelkie.connector.CassandraConnector;
import com.gpelkie.connector.QueryUtil;

import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by gpelkie on 1/21/19.
 */
public class BookRepositoryTest {

    private CassandraConnector connector;
    private static final String KEYSPACE = "library";

    @Before
    public void connect() throws Exception {
        EmbeddedCassandraServerHelper.startEmbeddedCassandra();
        connector = new CassandraConnector("127.0.0.1", 9142);
        connector.connect();
        String query = QueryUtil.generateCreateQuery(KEYSPACE);
        connector.getSession().execute(query);
    }

    @Test
    public void createTable() {
        BookRepository bookRepository = new BookRepository(connector.getSession(), KEYSPACE);
        bookRepository.createTable();

        ResultSet resultSet = connector.getSession().execute("SELECT * FROM " + KEYSPACE + "." + BookRepository.TABLE_NAME + ";");
        List<String> columnNames = resultSet.getColumnDefinitions().asList().stream()
                .map(ColumnDefinitions.Definition::getName)
                .collect(Collectors.toList());

        assertEquals(columnNames.size(), 3);
        assertTrue(columnNames.contains("id"));
        assertTrue(columnNames.contains("title"));
        assertTrue(columnNames.contains("subject"));
    }

    @Test
    public void testAddColumn() {
        BookRepository bookRepository = new BookRepository(connector.getSession(), KEYSPACE);
        bookRepository.createTable();
        bookRepository.appendColumn("publisher", "text");

        ResultSet resultSet = connector.getSession().execute("SELECT * FROM " + KEYSPACE + "." + BookRepository.TABLE_NAME + ";");

        assertTrue(resultSet.getColumnDefinitions().contains("publisher"));
    }

    @After
    public void tearDown() throws Exception {
        connector.close();
    }
}
