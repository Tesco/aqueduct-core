package Helper

import com.stehno.ersatz.Decoders
import com.stehno.ersatz.ErsatzServer

import static java.util.stream.Collectors.joining

class LocationMock {

    public final static String LOCATION_CLUSTER_PATH_WITH_QUERY_PARAM = "$LOCATION_CLUSTER_PATH?$CLUSTER_QUERY_PARAM=$CLUSTER_QUERY_PARAM_VALUE"
    public final static String LOCATION_CLUSTER_PATH_FILTER_PATTERN = "/**/some/get/*/clusters/path/**"

    private final static String LOCATION_CLUSTER_PATH = "/some/get/{locationUuid}/clusters/path"
    private final static String LOCATION_PATH = "/tescolocation"
    private final static String CLUSTER_QUERY_PARAM ="param"
    private final static String CLUSTER_QUERY_PARAM_VALUE ="P,Q,R,S"

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
            }.query(CLUSTER_QUERY_PARAM, CLUSTER_QUERY_PARAM_VALUE)
        }
    }

    private String locationPathIncluding(String locationUuid) {
        LOCATION_CLUSTER_PATH.replace("{locationUuid}", locationUuid)
    }
}
