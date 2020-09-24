package Helper

import com.stehno.ersatz.Decoders
import com.stehno.ersatz.ErsatzServer

import static java.util.stream.Collectors.joining

class LocationMock {

    private static final String LOCATION_PATH = "/tescolocation"
    private static final String LOCATION_CLUSTER_PATH = "/clusters/v1/locations/{locationUuid}/clusters/ids"

    private ErsatzServer locationMockService
    private String accessToken

    LocationMock(String accessToken) {
        this.accessToken = accessToken

        locationMockService = new ErsatzServer({
            decoder('application/json', Decoders.utf8String)
            reportToConsole()
        })

        locationMockService.start()
    }

    String getUrl() {
        return locationMockService.getHttpUrl() + "$LOCATION_PATH/"
    }

    void clearExpectations() {
        locationMockService.clearExpectations()
    }

    void getClusterForGiven(String locationUuid, List<String> clusters) {
        def clusterString = clusters.stream().map{"\"$it\""}.collect(joining(","))
        def revisionId = clusters.isEmpty() ? null : "2"

        locationMockService.expectations {
            get(LOCATION_PATH + locationPathIncluding(locationUuid)) {
                header("Authorization", "Bearer " + accessToken)
                called(1)

                responder {
                    header("Content-Type", "application/json")
                    body("""
                    {
                        "clusters": [$clusterString],
                        "revisionId": "$revisionId"
                    }
               """)
                }
            }
        }
    }

    private String locationPathIncluding(String locationUuid) {
        LOCATION_CLUSTER_PATH.replace("{locationUuid}", locationUuid)
    }
}
