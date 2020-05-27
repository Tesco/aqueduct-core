package com.tesco.aqueduct.registry.postgres;

import com.tesco.aqueduct.registry.model.*;
import com.tesco.aqueduct.registry.utils.RegistryLogger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.IOException;
import java.net.URL;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.stream.Collectors;

public class PostgreSQLNodeRegistry implements NodeRegistry {
    private static final Random random = new Random();
    private static final RegistryLogger LOG = new RegistryLogger(LoggerFactory.getLogger(PostgreSQLNodeRegistry.class));

    private final URL cloudUrl;
    private final Duration offlineDelta;
    private final DataSource dataSource;
    private final PostgresNodeGroupStorage nodeGroupStorage;

    public PostgreSQLNodeRegistry(final DataSource dataSource, final URL cloudUrl, final Duration offlineDelta) {
        this(dataSource, cloudUrl, offlineDelta, new PostgresNodeGroupStorage());
    }

    PostgreSQLNodeRegistry(final DataSource dataSource, final URL cloudUrl, final Duration offlineDelta, PostgresNodeGroupStorage nodeGroupStorage) {
        this.cloudUrl = cloudUrl;
        this.offlineDelta = offlineDelta;
        this.dataSource = dataSource;
        this.nodeGroupStorage = nodeGroupStorage;
    }

    @Override
    public List<URL> register(final Node nodeToRegister) {
        try (Connection connection = getConnection()) {
            final PostgresNodeGroup group = nodeGroupStorage.getNodeGroup(connection, nodeToRegister.getGroup());
            Node node = group.upsert(nodeToRegister, cloudUrl);

            group.processOfflineNodes(ZonedDateTime.now().minus(offlineDelta), cloudUrl);
            group.persist(connection);

            return node.getRequestedToFollow();
        } catch (SQLException | IOException exception) {
            LOG.error("Postgresql node registry", "register node", exception);
            throw new RuntimeException(exception);
        }
    }

    private Connection getConnection() throws SQLException {
        long start = System.currentTimeMillis();
        try {
            return dataSource.getConnection();
        } finally {
            long end = System.currentTimeMillis();
            LOG.info("getConnection:time", Long.toString(end - start));
        }
    }

    @Override
    public StateSummary getSummary(final long offset, final Status status, final List<String> groupIds) {
        List<PostgresNodeGroup> groups = getPostgresNodeGroups(groupIds);

        final ZonedDateTime threshold = ZonedDateTime.now().minus(offlineDelta);
        groups.forEach(group -> group.markNodesOfflineIfNotSeenSince(threshold));

        final List<Node> followers = groups.stream()
            .flatMap(nodeGroup -> nodeGroup.getNodes().stream()).collect(Collectors.toList());

        return new StateSummary(getCloudNode(offset, status), followers);
    }

    private List<PostgresNodeGroup> getPostgresNodeGroups(List<String> groupIds) {
        List<PostgresNodeGroup> groups;
        try (Connection connection = getConnection()) {
            groups = nodeGroupStorage.readNodeGroups(connection, groupIds);
        } catch (SQLException | IOException exception) {
            LOG.error("Postgresql node registry", "get summary", exception);
            throw new RuntimeException(exception);
        }
        return groups;
    }

    private Node getCloudNode(final long offset, final Status status) {
        return Node.builder()
            .localUrl(cloudUrl)
            .offset(offset)
            .status(status)
            .following(Collections.emptyList())
            .lastSeen(ZonedDateTime.now())
            .build();
    }

    @Override
    public boolean deleteNode(final String groupId, final String host) {
        try (Connection connection = getConnection()) {
            final PostgresNodeGroup nodeGroup = nodeGroupStorage.getNodeGroup(connection, groupId);

            if(nodeGroup.isEmpty()) {
                return false;
            } else {
                return deleteExistingNode(connection, host, nodeGroup);
            }
        } catch (SQLException | IOException exception) {
            LOG.error("Postgresql node registry", "deleteNode", exception);
            throw new RuntimeException(exception);
        }
    }

    private boolean deleteExistingNode(final Connection connection, final String host, final PostgresNodeGroup group) throws IOException, SQLException {
        final boolean foundNode = group.removeByHost(host);
        if (foundNode) {
            if (group.isEmpty()) {
                group.delete(connection);
            } else {
                group.updateGetFollowing(cloudUrl);
                group.persist(connection);
            }
            return true;
        }
        return false;
    }
}
