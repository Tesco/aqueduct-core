import com.tesco.aqueduct.pipe.http.PipeStatusController
import io.micronaut.context.ApplicationContext
import io.micronaut.inject.qualifiers.Qualifiers
import io.micronaut.runtime.server.EmbeddedServer
import io.restassured.RestAssured
import spock.lang.AutoCleanup
import spock.lang.Ignore
import spock.lang.Specification

import javax.sql.DataSource

import static org.hamcrest.Matchers.equalTo

@Ignore
class SwaggerIntegrationSpec extends Specification {
    @AutoCleanup("stop") ApplicationContext context

    def setup() {

        context = ApplicationContext
            .build()
            .properties(
                "pipe.server.url": "http://cloud.pipe",
                "persistence.read.limit": 1000,
                "persistence.read.retry-after": 10000,
                "persistence.read.max-batch-size": "10485760",
                "micronaut.router.static-resources.swagger.paths": "classpath:META-INF/swagger",
                "micronaut.router.static-resources.swagger.mapping": "/swagger/**",
                "micronaut.security.enabled": "false"
            )
            .mainClass(PipeStatusController)
            .build()
            .registerSingleton(DataSource, Mock(DataSource), Qualifiers.byName("pipe"))

        context.start()

        def server = context.getBean(EmbeddedServer)
        server.start()

        RestAssured.port = server.port
    }

    def cleanup() {
        RestAssured.port = RestAssured.DEFAULT_PORT
    }

    def "openapi yaml exists"() {
        given: "A running server"

        when: "we call to get swagger"
        def request = RestAssured.get("/swagger/aqueduct-1-0.yml")

        then: "response is correct"
        def response = """{"upToDate":true,"localOffset":"100"}"""
        request
            .then()
            .statusCode(200)
            .body(equalTo(response))
    }
}