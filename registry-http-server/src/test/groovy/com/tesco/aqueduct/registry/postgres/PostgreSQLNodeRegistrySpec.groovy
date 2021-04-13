package com.tesco.aqueduct.registry.postgres

import com.tesco.aqueduct.registry.model.Node
import spock.lang.Specification

import javax.sql.DataSource
import java.sql.Connection
import java.time.Duration

class PostgreSQLNodeRegistrySpec extends Specification {

	def "New node can register"() {
		given: "a mock node group"
		def cloudUrl = new URL("http://cloud.url")
		def mockNodeGroup = Mock(PostgresNodeGroup)

		1 * mockNodeGroup.upsert(_, _) >> Node.builder().requestedToFollow([cloudUrl]).build()
		def mockNodeGroupFactory = Mock(PostgresNodeGroupStorage)
		1 * mockNodeGroupFactory.getNodeGroup(_, _) >> mockNodeGroup

		and: "a mock data source"
		def dataSourceMock = Mock(DataSource)
		def mockConnection = Mock(Connection)
		2 * dataSourceMock.getConnection() >> mockConnection
		1 * mockConnection.setAutoCommit(false)

		and: "a node registry"
		def offlineDelta = Duration.ofMinutes(5)
		def removeDelta = Duration.ofMinutes(10)
		def registry = new PostgreSQLNodeRegistry(dataSourceMock, cloudUrl, offlineDelta, removeDelta, mockNodeGroupFactory)

		and: "a node to register"
		def testNode = Mock(Node)

		when: "registering the node"
		def result = registry.register(testNode)

		then: "the cloud url is returned"
		result.requestedToFollow == [cloudUrl]
	}
}
