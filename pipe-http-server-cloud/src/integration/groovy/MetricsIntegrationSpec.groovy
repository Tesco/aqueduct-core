
import com.tesco.aqueduct.pipe.TestAppender
import io.micronaut.context.ApplicationContext
import io.micronaut.context.env.yaml.YamlPropertySourceLoader
import io.micronaut.inject.qualifiers.Qualifiers
import io.micronaut.runtime.server.EmbeddedServer
import io.restassured.RestAssured
import spock.lang.AutoCleanup
import spock.lang.Shared
import spock.lang.Specification
import javax.sql.DataSource

class MetricsIntegrationSpec extends Specification {

    @Shared @AutoCleanup("stop") ApplicationContext context

    void setupSpec() {
        context = ApplicationContext
            .build()
            .mainClass(EmbeddedServer)
            .properties(
                parseYamlConfig(
                """
                micronaut:
                  security:
                    enabled: true
                endpoints:
                  all:
                    enabled: true
                    path: /endpoints
                    sensitive: true
                    
                """
                )
            )
            .build()
            .registerSingleton(DataSource, Mock(DataSource), Qualifiers.byName("pipe"))
            .registerSingleton(DataSource, Mock(DataSource), Qualifiers.byName("registry"))

        context.start()

        def server = context.getBean(EmbeddedServer)
        server.start()

        RestAssured.port = server.port
    }

    void cleanup() {
        TestAppender.clearEvents()
    }

    def "Metrics are dumped through logs using DumpMetrics component"() {
        expect: "metrics are logged"
        TestAppender.getEvents().stream()
            .anyMatch {
                it.loggerName.contains("metrics")
            }
    }

    def "metric endpoint return unauthorised when no authentication is provided"() {
        expect: "endpoint return 401"
        RestAssured.get("/endpoints/metrics").thenReturn().statusCode() == 401
    }

    Map<String, Object> parseYamlConfig(String str) {
        def loader = new YamlPropertySourceLoader()
        loader.read("config", str.bytes)
    }
}
