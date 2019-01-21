package com.gpelkie.connector;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by gpelkie on 1/21/19.
 */
public class CassandraConnector {

    private Logger log = LoggerFactory.getLogger(CassandraConnector.class);

    private Cluster cluster;
    private Session session;

    private final String node;
    private final Integer port;

    /**
     * Cassandra connector constructor.
     *
     * @param node the Cassandra node IP Address
     * @param port the port of the cluster host
     */
    public CassandraConnector(final String node, final Integer port) {
        this.node = node;
        this.port = port;
    }

    /**
     * Connect to the Cassandra Cluster specified by the ip address and port provided.
     */
    public void connect() {
        Cluster.Builder cBuilder = Cluster.builder().addContactPoint(node);
        if (port != null) {
            cBuilder.withPort(port);
        }
        cluster = cBuilder.build();
        session = cluster.connect();

        log.info("Connected to cluster {}", cluster.getMetadata().getClusterName());
    }

    /**
     * Provide the session.
     *
     * @return the session
     */
    public Session getSession() {
        return this.session;
    }

    /**
     * Close the cluster.
     */
    public void close() {
        session.close();
        cluster.close();
    }
}
