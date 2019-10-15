package com.tesco.aqueduct.registry.postgres

import spock.lang.Specification

import com.tesco.aqueduct.registry.model.Node

import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet

class PostgresNodeGroupSpec extends Specification {
    def "can fetch a NodeGroup"() {
        String statement
        given: "a SQL connection"
        def connection = Mock(Connection) {
            1* prepareStatement(_) >> {
                statement = it
                return Mock(PreparedStatement) {
                    1 * executeQuery() >> Mock(ResultSet) {
                        1* next() >> true
                        1* getString("entry") >>
                            "[" +
                            "{" +
                            "\"localUrl\":\"http://node-1\"," +
                            "\"offset\":\"0\"," +
                            "\"providerLastAckOffset\":\"0\"," +
                            "\"id\":\"http://node-1\"" +
                            "}" +
                            "]"
                        1* getInt("version") >> 99
                        1* getString("group_id") >> "test-group-id-from-db"
                    }
                }
            }
        }
        when: "I ask the factory to create a NodeGroup"
        def result = PostgresNodeGroup.getNodeGroup(connection, "test-group-id")
        then: "then a select statement is run"
        statement.contains("SELECT")
        and: "The NodeGroup is correctly created"
        result.nodes[0].localUrl == new URL("http://node-1")
        result.version == 99
        result.groupId == "test-group-id-from-db"
    }

    def "can update groups"() {
        String statement
        given: "a SQL connection"
        def connection = Mock(Connection) {
            1* prepareStatement(_) >> {
                statement = it
                return Mock(PreparedStatement) {
                    1 * executeUpdate() >> 1
                }
            }
        }
        and: "a PostgresNodeGroup"
        Node n1 = Node.builder()
            .localUrl(new URL("http://node-1"))
            .build()
        def group = new PostgresNodeGroup("test-id", 99, [n1])
        when: "we update the node group"
        group.persist(connection)
        then: "a statement has been run against the SQL connection"
        statement.contains("UPDATE")
    }

    def "If an update fails, then throw an exception"() {
        given: "a failing SQL connection"
        def connection = Mock(Connection) {
            1* prepareStatement(_) >> Mock(PreparedStatement) {
                1* executeUpdate() >> 0
            }
        }
        and: "a PostgresNodeGroup"
        Node n1 = Node.builder()
            .localUrl(new URL("http://node-1"))
            .build()
        def group = new PostgresNodeGroup("test-id", 99, [n1])
        when: "we update the node group"
        group.persist(connection)
        then: "a Version Changed Exception is thrown"
        thrown(VersionChangedException)
    }

    def "can delete groups"() {
        String statement
        given: "a SQL connection"
        def connection = Mock(Connection) {
            1* prepareStatement(_) >> {
                statement = it
                return Mock(PreparedStatement) {
                    1 * executeUpdate() >> 1
                }
            }
        }
        and: "a PostgresNodeGroup"
        Node n1 = Node.builder()
            .localUrl(new URL("http://node-1"))
            .build()
        def group = new PostgresNodeGroup("test-id", 99, [n1])
        when: "we delete the node group"
        group.delete(connection)
        then: "a statement has been run against the SQL connection"
        statement.contains("DELETE")
    }

    def "If a delete fails, then throw an exception"() {
        given: "a failing SQL connection"
        def connection = Mock(Connection) {
            1* prepareStatement(_) >> Mock(PreparedStatement) {
                1* executeUpdate() >> 0
            }
        }
        and: "a PostgresNodeGroup"
        Node n1 = Node.builder()
            .localUrl(new URL("http://node-1"))
            .build()
        def group = new PostgresNodeGroup("test-id", 99, [n1])
        when: "we delete the node group"
        group.delete(connection)
        then: "a Version Changed Exception is thrown"
        thrown(VersionChangedException)
    }

    def "can insert groups"() {
        String statement
        given: "a SQL connection"
        def connection = Mock(Connection) {
            1* prepareStatement(_) >> {
                statement = it
                return Mock(PreparedStatement) {
                    1 * executeUpdate() >> 1
                }
            }
        }
        and: "a PostgresNodeGroup"
        Node n1 = Node.builder()
            .localUrl(new URL("http://node-1"))
            .build()
        def group = new PostgresNodeGroup("test-id")
        group.add(n1, new URL("http://cloud"))
        when: "we insert the node group"
        group.persist(connection)
        then: "a statement has been run against the SQL connection"
        statement.contains("INSERT")
    }

    def "If an insert fails, then throw an exception"() {
        given: "a failing SQL connection"
        def connection = Mock(Connection) {
            1* prepareStatement(_) >> Mock(PreparedStatement) {
                1* executeUpdate() >> 0
            }
        }
        and: "a PostgresNodeGroup"
        Node n1 = Node.builder()
            .localUrl(new URL("http://node-1"))
            .build()
        def group = new PostgresNodeGroup("test-id", 99, [n1])
        when: "we insert the node group"
        group.persist(connection)
        then: "a Version Changed Exception is thrown"
        thrown(VersionChangedException)
    }
}
