package com.gpelkie.connector;

import com.datastax.driver.core.ResultSet;

import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;


/**
 * Created by gpelkie on 1/21/19.
 */
public class CassandraConnectorTest {

    private CassandraConnector connector;

    @Before
    public void connect() throws Exception {
        EmbeddedCassandraServerHelper.startEmbeddedCassandra();
        connector = new CassandraConnector("127.0.0.1", 9142);
        connector.connect();
    }

    @Test
    public void testCreateKeyspace() {
        final String keyspaceName = "library";
        String query = QueryUtil.generateCreateQuery(keyspaceName);
        connector.getSession().execute(query);

        ResultSet resultSet = connector.getSession().execute("SELECT * FROM system_schema.keyspaces;");

        List<String> matchedKeyspaces = resultSet.all().stream()
                .filter(row -> row.getString(0).equalsIgnoreCase(keyspaceName))
                .map(row -> row.getString(0))
                .collect(Collectors.toList());

        assertEquals(matchedKeyspaces.size(), 1);
        assertEquals(matchedKeyspaces.get(0), keyspaceName.toLowerCase());
    }

    @After
    public void tearDown() throws Exception {
        connector.close();
    }
}
