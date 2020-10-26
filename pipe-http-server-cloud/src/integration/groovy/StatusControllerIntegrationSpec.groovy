import com.tesco.aqueduct.pipe.http.StatusController
import com.tesco.aqueduct.pipe.http.Version
import io.micronaut.context.ApplicationContext
import io.micronaut.http.HttpStatus
import io.micronaut.inject.qualifiers.Qualifiers
import io.micronaut.runtime.server.EmbeddedServer
import io.restassured.RestAssured
import spock.lang.AutoCleanup
import spock.lang.Shared
import spock.lang.Specification
import javax.sql.DataSource

import static io.restassured.RestAssured.when
import static org.hamcrest.Matchers.equalTo

class StatusControllerIntegrationSpec extends Specification {

    @Shared @AutoCleanup("stop") ApplicationContext context
    @Shared @AutoCleanup("stop") EmbeddedServer server

    void setupSpec() {
        context = ApplicationContext
            .build()
            .mainClass(StatusController)
            .build()
            .registerSingleton(DataSource, Mock(DataSource), Qualifiers.byName("pipe"))
            .registerSingleton(DataSource, Mock(DataSource), Qualifiers.byName("registry"))

        context.start()

        server = context.getBean(EmbeddedServer)
        server.start()

        RestAssured.port = server.port
    }

    void cleanupSpec() {
        RestAssured.port = RestAssured.DEFAULT_PORT
    }

    def "pipe status contains version in it"() {
        expect:
        when()
            .get("/pipe/_status")
            .then()
            .statusCode(HttpStatus.OK.code)
            .body("version", equalTo(Version.getImplementationVersion()))
    }
}
