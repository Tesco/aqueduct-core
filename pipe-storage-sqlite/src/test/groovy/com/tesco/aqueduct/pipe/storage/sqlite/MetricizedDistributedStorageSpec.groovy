package com.tesco.aqueduct.pipe.storage.sqlite


import com.tesco.aqueduct.pipe.api.OffsetEntity
import com.tesco.aqueduct.pipe.api.OffsetName
import com.tesco.aqueduct.pipe.api.PipeEntity
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import spock.lang.Specification
import spock.lang.Unroll

class MetricizedDistributedStorageSpec extends Specification {

    private MetricizedDistributedStorage metricizedDistributedStorage
    def timedDistributedStorage = Mock(TimedDistributedStorage)
    def meterRegistry = new SimpleMeterRegistry()

    @Unroll
    def "write offset is recorded as gauge metric"() {
        given: "a metricized distributed storage and an offset entity"
        metricizedDistributedStorage = new MetricizedDistributedStorage(timedDistributedStorage, meterRegistry)
        def offsetEntity = new OffsetEntity(offsetName, OptionalLong.of(offsetValue))

        when: "an offset is written"
        metricizedDistributedStorage.write(offsetEntity)

        then: "the offset metric is recorded and the timed distributed storage is invoked"
        meterRegistry.find(metricName)?.meter()?.measure()?.first()?.value == offsetValue
        1 * timedDistributedStorage.write(offsetEntity)

        where:
        metricName                          | offsetName                        | offsetValue
        "pipe.offset.globalLatestOffset"    | OffsetName.GLOBAL_LATEST_OFFSET   | 100L
        "pipe.offset.localLatestOffset"     | OffsetName.LOCAL_LATEST_OFFSET    | 15L
        "pipe.offset.pipeOffset"            | OffsetName.PIPE_OFFSET            | 54465L
    }

    def "write offset is recorded as gauge metric when pipe entity is written"() {
        given: "a metricized distributed storage and an offset entity"
        metricizedDistributedStorage = new MetricizedDistributedStorage(timedDistributedStorage, meterRegistry)
        PipeEntity pipeEntity = new PipeEntity(
            null,
            [
                new OffsetEntity(OffsetName.GLOBAL_LATEST_OFFSET, OptionalLong.of(100L)),
                new OffsetEntity(OffsetName.LOCAL_LATEST_OFFSET, OptionalLong.of(15L)),
                new OffsetEntity(OffsetName.PIPE_OFFSET, OptionalLong.of(54465L))
            ],
            null)

        when: "an offset is written"
        metricizedDistributedStorage.write(pipeEntity)

        then: "the offset metric is recorded and the timed distributed storage is invoked"
        meterRegistry.find("pipe.offset.globalLatestOffset")?.meter()?.measure()?.first()?.value == 100L
        meterRegistry.find("pipe.offset.localLatestOffset")?.meter()?.measure()?.first()?.value == 15L
        meterRegistry.find("pipe.offset.pipeOffset")?.meter()?.measure()?.first()?.value == 54465L

        and:
        1 * timedDistributedStorage.write(pipeEntity)
    }
}
