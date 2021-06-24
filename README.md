# Blockchain connector for corda
- FireFly Cordapp
- Connector springboot application
  - Exposes REST endpoint to interact with firefly cordapp
  - Exposes REST endpoints to manage eventstreams and subscriptions
  - Exposes Websocket endpoint to consume events from corda ledger 
## PRE-REQS
- JDK 11 (such as from [https://adoptopenjdk.net/](https://gradle.org/install/))
- gradle ([https://gradle.org/install/](https://gradle.org/install/))

## BUILD
```
./gradlew build
```

The outputs are in these folders:

- Cordapp jars
  - `cordapp/firefly-contracts/build/libs/firefly-contracts.jar`
  - `cordapp/firefly-flows/build/libs/firefly-flows.jar`
- Connector springboot application
  - `connector/build/libs/connector.jar`
  
## Running locally

### Prerequisites

- Install above cordapps on corda nodes in your corda network

### Running connector locally

- Run following command to run the connector springboot application

```
./gradlew bootRun --args='--rpc.host=<rpc host for corda node> --rpc.username=test --rpc.password=client123!'
```

- Open `http://localhost:8080/swagger-ui/index.html` to view rest endpoints available.
- Create an eventstream using `POST /eventstream`, example request body

```
{
  "data": {
    "name": "eventstream-0",
    "batchSize": 10,
    "batchTimeoutMS": 5000,
    "blockedRetryDelaySec": 20,
    "errorHandling": "BLOCK",
    "websocket": {
      "topic": "eventstream-0-topic"
    }
  }
}
```

- Note the eventstream id from the response

```
{
  "statusCode": 200,
  "status": true,
  "message": "SUCCESS",
  "data": {
    "id": "es-622a2ff8-b688-46b2-874b-de7c5cd4e8dc",
    "name": "eventstream-0",
    "batchSize": 10,
    "batchTimeoutMs": 5000,
    "batchRetryDelaySec": 20,
    "errorHandling": "BLOCK",
    "websocketTopic": "eventstream-0-topic",
    "suspended": false,
    "created": "2021-06-24T19:49:53.775Z",
    "updated": null
  }
}
```

- Create an subscription using `POST /subscriptions`, example request body using id of eventstream created in previous step

```
{
  "data": {
    "fromTime": "2021-06-24T19:54:17.776Z",
    "stream": "es-622a2ff8-b688-46b2-874b-de7c5cd4e8dc",
    "filter": {
      "stateType": "io.kaleido.firefly.cordapp.states.BroadcastBatch",
      "stateStatus": "UNCONSUMED",
      "relevancyStatus": "RELEVANT"
    },
    "name": "subscription-0"
  }
}
```

- response 

```
{
  "statusCode": 200,
  "status": true,
  "message": "SUCCESS",
  "data": {
    "id": "sb-6dd6e475-b2ed-4109-9831-b259e9b08ef2",
    "name": "subscription-0",
    "stream": {
      "id": "es-622a2ff8-b688-46b2-874b-de7c5cd4e8dc",
      "name": "eventstream-0",
      "batchSize": 10,
      "batchTimeoutMs": 5000,
      "batchRetryDelaySec": 20,
      "errorHandling": "BLOCK",
      "websocketTopic": "eventstream-0-topic",
      "suspended": false,
      "created": "2021-06-24T19:49:53.775Z",
      "updated": null
    },
    "stateType": "io.kaleido.firefly.cordapp.states.BroadcastBatch",
    "stateStatus": "UNCONSUMED",
    "relevancyStatus": "RELEVANT",
    "fromTime": "2021-06-24T19:54:17.776Z",
    "lastCheckpoint": null,
    "created": "2021-06-24T19:56:15.845Z",
    "updated": null
  }
}
```

- setup websocket client to listen to corda events, you can use following node sample

```
const WebSocket = require('ws')
const ws = new WebSocket(`ws://localhost:8080/ws`);
let lastAck = null;
let heartBeatTimeout = setTimeout(() => {
  console.error('Event stream ping timeout');
  ws.terminate();
}, 10000);
const heartBeat = () => {
  ws.ping();
  clearTimeout(heartBeatTimeout);
  heartBeatTimeout = setTimeout(() => {
    console.error('Event stream ping timeout');
    ws.terminate();
  }, 2000);
}

ws.on('open', () => {
  ws.send(JSON.stringify({
    type: 'listen',
    topic: 'eventstream-0-topic'
  }));
  heartBeat();
}).on('close', () => {
  console.error(`Event stream websocket disconnected`);
}).on('message', async (message) => {
  const data = JSON.parse(message);
  for(var i=0; i<data.length; i++) { 
    console.log(JSON.stringify(data[i]));
  }
  ws.send(JSON.stringify({
    type: 'ack',
    topic: 'eventstream-0-topic'
  }));
}).on('pong', () => {
  heartBeat();
}).on('error', err => {
  console.error(`Event stream websocket error. ${err}`);
});
```

- send a firefly transaction using `POST /broadcastBatch`, request body

```
{
  "data": {
    "batchId": "batch-id-0",
    "payloadRef": "some-payload-ref-0",
    "observers": [
      "CN=Node of u0xrgv6atc for u0fifdgpl8, O=Kaleido, L=Raleigh, C=US"
    ],
    "groupId": "3fa85f64-5717-4562-b3fc-2c963f66afa6"
  }
}
```

- you would receive transaction hash as response 
```
{
  "statusCode": 200,
  "status": true,
  "message": "SUCCESS",
  "data": "891F8893DB10130413C4D504F42395C9D8C788684074C325DCE115E8DBF915CE"
}
```

- On websocket client you would receive the event message from corda

```
{
    "data":
    {
        "data":
            {
                "@class":"io.kaleido.firefly.cordapp.states.BroadcastBatch",
                "author":"CN=Node of u0jh5fc7yc for u0fifdgpl8, O=Kaleido, L=Raleigh, C=US",
                "batchId":"batch-id-0",
                "payloadRef":"some-payload-ref-0",
                "participants":["CN=Node of u0xrgv6atc for u0fifdgpl8, O=Kaleido, L=Raleigh, C=US","CN=Node of u0jh5fc7yc for u0fifdgpl8, O=Kaleido, L=Raleigh, C=US"]
            },
            "contract":"io.kaleido.firefly.cordapp.contracts.FireflyContract",
            "notary":"CN=Node of u0congcs2l for u0fifdgpl8, O=Kaleido, L=Raleigh, C=US",
            "encumbrance":null,
            "constraint":
            {
                "@class":"net.corda.core.contracts.SignatureAttachmentConstraint",
                "key":"3mviYCPz42wQCX8yUe3fdhFKHg7ZDA1dthqUQF946tgfUC43hosoa8Ef98JXufsSVMW8C6L3UEHhh4kdKr1xashDNWuacM5F43dL9Btzs534H2iEksEDX3aWWAMgXBMj88jiEmQi7orS2VSrpB29QNGYTKWZrcKrK4oJm3bYNKiv4Smrkd5zWoZQ8Tzz2HKamvSXqQ2s3MxTYXZrJ6zTz2rEMjXBF3s1d8tPqV3LBhvpDwG4MaVhg3UREzKkZGe58T8x1QJ4GEu6TGU43T3VJ295dyZSEguXj6v1g6HVqTPE8CnLvszy31E3xSBGU2sQFNeC"
            }
        },
        "subId":"sb-6dd6e475-b2ed-4109-9831-b259e9b08ef2",
        "signature":"io.kaleido.firefly.cordapp.states.BroadcastBatch",
        "stateRef":
        {
            "txhash":"891F8893DB10130413C4D504F42395C9D8C788684074C325DCE115E8DBF915CE",
            "index":0
        },
        "recordedTime":"2021-06-24T20:04:30.361398Z",
        "consumedTime":null
}
```

## TODOS
- Solve broadcast problem for corda
    - All communication is corda is point to point, and there is no concept of global blockchain state in corda.
    - We currently solve it by passing a list of observers (list of corda nodes)
    - This solution has a late join problem
- add support for async requests 
- add support for request retries similaer to ethconnect
- e2e integration with firefly
- Add unit/integration tests and coverage 
- Add acceptance tests





 



