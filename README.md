# Object store REST API

This application implements a service to store objects organized in buckets. Each object and bucket is identified by and ID.
The objects are de-duplicated by bucket and can be stored in memory or on files.

The service exposes a REST API to perform action on the objects.

### Available actions and endpoints

*NOTE*: object ID and bucket ID must match the following regex: `[a-z0-9_-]+`.
Or they can be made up of numbers, lowercase letters, underscores and dashes.

#### Store:
`PUT /objects/<bucketId>/<objId>`

Stores an object in a bucket, the object data is the content of the body of the request (the `Content-Type` of the request must be `text/plain`).
The service replies with a `201` on object creation or a `200` on object replacement and .

If the bucket does not exist it is created. If the object is already present, it is replaced.

#### Retrieve:
`GET /objects/<bucketId>/<objId>`

Retrieves an object in a bucket. The service replies with a `200` and the object as the body if it is found in the storage.
It replies with a `404` if the object or the bucket is not in the storage.

#### Delete
`DELETE /objects/<bucketId>/<objId>`

Deletes an object from a bucket. The service replies with a `200` if the object is deleted
or a `404` if the object or the bucket is not in the storage.

---
### Build application
The application has been tested using go 1.17 with go modules

`go build -o objectstore-restapi github.com/flaviopicci/simple-objectstore-restapi/cmd/objectstore-restapi`

---
### Launch application

The service can be configured via command line options, configuration file or env variables.
Available options:
```
-v, --verbose          Print verbose output
-c, --config           Path to the configuration file
-l, --listen-address   Address to listen to in the form of <port> or <address>:<port>
-p, --persist          Use persistent storage to store objects
--data-path            Path to folder of persistent data
```

##### Examples:
Listen on localhost port 80 with persistent storage in /tmp/data

`./objectstore-restapi -l 127.0.0.1:80 -p --data-path /tmp/data`

Then store an object `object data` with ID `objy` in bucket `bucx`:
```
curl -v -d 'object data' -H "Content-Type: text/plain" -X PUT http://localhost:8080/objects/bucx/objy

...
< HTTP/1.1 201 Created
...
{"id":"objy"}
```

Retrieve it:
```
curl -v http://localhost:8080/objects/bucx/objy

...
< HTTP/1.1 200 OK
...
object data
```

Delete it:
```
curl -v -X DELETE http://localhost:8080/objects/bucx/objy

...
< HTTP/1.1 200 OK
...
```
