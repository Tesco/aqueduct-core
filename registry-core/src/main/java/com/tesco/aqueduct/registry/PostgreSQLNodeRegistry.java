package com.tesco.aqueduct.registry;

import com.tesco.aqueduct.registry.model.Node;
import com.tesco.aqueduct.registry.postgres.PostgresNodeGroup;
import com.tesco.aqueduct.registry.model.StateSummary;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.IOException;
import java.net.URL;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class PostgreSQLNodeRegistry implements NodeRegistry {

    private final URL cloudUrl;
    private final Duration offlineDelta;
    private final DataSource dataSource;
    private static final int OPTIMISTIC_LOCKING_COOLDOWN = 5;

    private static final RegistryLogger LOG = new RegistryLogger(LoggerFactory.getLogger(PostgreSQLNodeRegistry.class));

    public PostgreSQLNodeRegistry(final DataSource dataSource, final URL cloudUrl, final Duration offlineDelta) {
        this.cloudUrl = cloudUrl;
        this.offlineDelta = offlineDelta;
        this.dataSource = dataSource;
    }

    @Override
    public List<URL> register(final Node nodeToRegister) {
        int count = 0;
        while (true) {
            try (Connection connection = getConnection()) {
                connection.setAutoCommit(false);
                final PostgresNodeGroup group = PostgresNodeGroup.getNodeGroup(connection, nodeToRegister.getGroup());
                Node node = group.getById(nodeToRegister.getId());
                if (node != null) {
                    node = updateNodeToFollow(nodeToRegister, node.getRequestedToFollow());
                    group.updateNode(node);
                } else {
                    node = group.add(nodeToRegister, cloudUrl);
                }
                group.persist(connection);
                connection.commit();
                return node.getRequestedToFollow();
            } catch (SQLException | IOException exception) {
                LOG.error("Postgresql node registry", "register node", exception);
                throw new RuntimeException(exception);
            } catch (VersionChangedException exception) {
                try {
                    LOG.info("Postgresql version changed exception", Integer.toString(++count));
                    Thread.sleep(OPTIMISTIC_LOCKING_COOLDOWN);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
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
    public StateSummary getSummary(final long offset, final String status, final List<String> groupIds) {
        List<PostgresNodeGroup> groups = getPostgresNodeGroups(groupIds);

        final ZonedDateTime threshold = ZonedDateTime.now().minus(offlineDelta);
        groups.forEach(group -> group.markNodesOfflineIfNotSeenSince(threshold));

        final List<Node> followers = groups.stream()
            .map(group -> group.nodes)
            .flatMap(Collection::stream)
            .collect(Collectors.toList());

        return new StateSummary(getCloudNode(offset, status), followers);
    }

    private List<PostgresNodeGroup> getPostgresNodeGroups(List<String> groupIds) {
        List<PostgresNodeGroup> groups;
        try (Connection connection = getConnection()) {
            if (groupIds == null || groupIds.isEmpty()) {
                groups = PostgresNodeGroup.getNodeGroups(connection);
            } else {
                groups = PostgresNodeGroup.getNodeGroups(connection, groupIds);
            }
        } catch (SQLException exception) {
            LOG.error("Postgresql node registry", "get summary", exception);
            throw new RuntimeException(exception);
        }
        return groups;
    }

    private Node getCloudNode(final long offset, final String status) {
        return Node.builder()
            .localUrl(cloudUrl)
            .offset(offset)
            .status(status)
            .following(Collections.emptyList())
            .lastSeen(ZonedDateTime.now())
            .build();
    }

    @Override
    public boolean deleteNode(final String groupId, final String nodeId) {
        while (true) {
            try (Connection connection = getConnection()) {
                final PostgresNodeGroup nodeGroup = PostgresNodeGroup.getNodeGroup(connection, groupId);

                if(nodeGroup.isEmpty()) {
                    return false;
                } else {
                    return deleteExistingNode(connection, nodeId, nodeGroup);
                }
            } catch (SQLException | IOException exception) {
                LOG.error("Postgresql node registry", "deleteNode", exception);
                throw new RuntimeException(exception);
            } catch (VersionChangedException exception) {
                try {
                    Thread.sleep(OPTIMISTIC_LOCKING_COOLDOWN);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    private boolean deleteExistingNode(final Connection connection, final String nodeId, final PostgresNodeGroup group) throws IOException, SQLException {
        final boolean foundNode = group.removeById(nodeId);
        if (foundNode) {
            if (group.isEmpty()) {
                group.delete(connection);
            } else {
                group.rebalance(cloudUrl);
                group.persist(connection);
            }
            return true;
        }
        return false;
    }

    private Node updateNodeToFollow(final Node node, final List<URL> followURLs) {
        return node.toBuilder()
            .requestedToFollow(followURLs)
            .lastSeen(ZonedDateTime.now())
            .build();
    }
}
