import com.tesco.aqueduct.pipe.http.CompactionTask
import com.tesco.aqueduct.pipe.storage.PostgresqlStorage
import io.micrometer.core.instrument.MeterRegistry
import spock.lang.Specification

class CompactionTaskSpec extends Specification {

    PostgresqlStorage postgresqlStorage = Mock()
    MeterRegistry registry = Mock {
        more() >> Mock(MeterRegistry.More)
    }

    def "error thrown for invalid cron expression passed onto CompactionTask"() {
        given:
        String cronExpWith4FieldsInsteadOf6 = "4 3 * *"

        when:
        new CompactionTask(registry, postgresqlStorage, cronExpWith4FieldsInsteadOf6)

        then:
        def illegalArgException = thrown(IllegalArgumentException)

        and:
        illegalArgException.message.contains("Invalid cron expression [$cronExpWith4FieldsInsteadOf6]")
    }

    def "no error thrown for valid cron expression"() {
        given:
        String validCronExpression = "0 14 0/1 * * ?"

        when:
        new CompactionTask(registry, postgresqlStorage, validCronExpression)

        then:
        noExceptionThrown()
    }
}
