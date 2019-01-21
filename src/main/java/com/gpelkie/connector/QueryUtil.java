package com.gpelkie.connector;

/**
 * Created by gpelkie on 1/21/19.
 */
public class QueryUtil {
    private static final String CREATE_IN_DNE = "CREATE KEYSPACE IF NOT EXISTS ";

    /**
     * Generate a query to create the specified Keyspace using the default strategy (SimpleStrategy) and replication
     * factor (1).
     *
     * @param name The keyspace name.
     *
     * @return The generated query.
     */
    public static String generateCreateQuery(String name) {
        return generateCreateQuery(name, "SimpleStrategy");
    }

    /**
     * Generate a query to create the specified Keyspace using the default replication factor (1).
     *
     * @param name                The keyspace name.
     * @param replicationStrategy The replication strategy.
     *
     * @return The generated query.
     */
    public static String generateCreateQuery(String name, String replicationStrategy) {
        return generateCreateQuery(name, replicationStrategy, 1);
    }

    /**
     * Generate a query to create the specified Keyspace.
     *
     * @param name                The keyspace name.
     * @param replicationStrategy The replication strategy.
     * @param replicationFactor   The replication factor.
     *
     * @return The generated query.
     */
    public static String generateCreateQuery(String name, String replicationStrategy, int replicationFactor) {
        StringBuilder sb = new StringBuilder(CREATE_IN_DNE)
                .append(name)
                .append(" WITH replication = {")
                .append("'class':'").append(replicationStrategy)
                .append("','replication_factor':").append(replicationFactor)
                .append("};");

        return sb.toString();
    }
}
