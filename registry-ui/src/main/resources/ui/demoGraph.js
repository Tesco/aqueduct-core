let demoGraph = {
    "root": {
        "localUrl": "http://demo",
        "offset": "123",
        "status": "ok",
        "lastSeen": "2019-02-07T22:32:46.955Z",
        "id": "http://demo"
    },
    "followers": [
    {
        "group": "6735",
        "localUrl": "http://1.1.1.0",
        "offset": "110",
        "status": "following",
        "following": [
            "http://demo"
        ],
        "requestedToFollow": [
            "http://demo"
        ],
        "lastSeen": "2019-02-07T22:32:27.15Z",
        "id": "6735|http://1.1.1.0",
        "provider": {
            "lastOffsetSent": 110,
            "lastAckedOffset": 109,
            "latestMessageLatency": 10,
            "timeOfLastSent": "2019-05-21T16:47:42.815Z",
            "timeOfLastAck": "2019-05-21T16:47:40.815Z"
        }
    },
    {
        "group": "6735",
        "localUrl": "http://1.1.1.1",
        "offset": "108",
        "status": "following",
        "following": [
            "http://1.1.1.0",
            "http://demo"
        ],
        "requestedToFollow": [
            "http://1.1.1.0",
            "http://demo"
        ],
        "lastSeen": "2019-02-07T22:32:27.165Z",
        "id": "6735|http://1.1.1.1",
        "provider": {
            "lastOffsetSent": 108,
            "lastAckedOffset": 108,
            "latestMessageLatency": 10,
            "timeOfLastSent": "2019-05-21T16:47:42.815Z",
            "timeOfLastAck": "2019-05-21T16:47:40.815Z"
        }
    },
    {
        "group": "6735",
        "localUrl": "http://1.1.1.2",
        "offset": "104",
        "status": "following",
        "following": [
            "http://1.1.1.0",
            "http://demo"
        ],
        "requestedToFollow": [
            "http://1.1.1.0",
            "http://demo"
        ],
        "lastSeen": "2019-02-07T22:32:27.172Z",
        "id": "6735|http://1.1.1.2",
        "provider": {
            "lastOffsetSent": 104,
            "lastAckedOffset": 104,
            "latestMessageLatency": 10,
            "timeOfLastSent": "2019-05-21T16:47:42.815Z",
            "timeOfLastAck": "2019-05-21T16:47:40.815Z"
        }
    },
    {
        "group": "6735",
        "localUrl": "http://1.1.1.3",
        "offset": "10",
        "status": "initialising",
        "following": [
            "http://1.1.1.0",
            "http://demo"
        ],
        "requestedToFollow": [
            "http://1.1.1.1",
            "http://1.1.1.0",
            "http://demo"
        ],
        "lastSeen": "2019-02-07T22:32:27.177Z",
        "id": "6735|http://1.1.1.3"
    },
    {
        "group": "6735",
        "localUrl": "http://1.1.1.4",
        "offset": "50",
        "status": "stale",
        "following": [
            "http://1.1.1.1",
            "http://1.1.1.0",
            "http://demo"
        ],
        "requestedToFollow": [
            "http://1.1.1.1",
            "http://1.1.1.0",
            "http://demo"
        ],
        "lastSeen": "2019-02-07T22:32:27.183Z",
        "id": "6735|http://1.1.1.4"
    },
    {
        "group": "6735",
        "localUrl": "http://1.1.1.5",
        "offset": "104",
        "status": "following",
        "following": [
            "http://1.1.1.0",
            "http://demo"
        ],
        "requestedToFollow": [
            "http://1.1.1.2",
            "http://1.1.1.0",
            "http://demo"
        ],
        "lastSeen": "2019-02-07T22:32:27.189Z",
        "id": "6735|http://1.1.1.5"
    }
    ]
}