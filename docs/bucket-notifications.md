# Bucket Notifications

## Bucket Notification Configuration

Bucket's notifications can be configured with the s3api operation [put-bucket-notification-configuration](https://awscli.amazonaws.com/v2/documentation/api/latest/reference/s3api/put-bucket-notification-configuration.html).
Specify notifications under the "TopicConfigurations" field, which is an array of jsons, one for each notification.
A notification json has these fields:
- Id: Mandatory. A unique string identifying the notification configuration.
- Events: Optional. An array of [events](https://docs.aws.amazon.com/AmazonS3/latest/userguide/notification-how-to-event-types-and-destinations.html) for which the notification is relevant.
          If not specified, the notification is relevant for all events.
- TopicArn: The connection file name. (To specify a Kafka target topic, see "Kafka Connection Fields" below).

Example for a bucket's notification configuration, in a file:
{
    "TopicConfigurations": [
        {
	    "Id": "created_from_s3op",
            "TopicArn": "connect.json",
            "Events": [
                "s3:ObjectCreated:*"
            ]
        }
    ]
}


## Connection File
A connection file contains some fields that specify the target notification server.
The connection file name is specified in TopicArn field of the [notification configuration](https://awscli.amazonaws.com/v2/documentation/api/latest/reference/s3api/put-bucket-notification-configuration.html)
In a containerized environment, the operator will mount the secrets as file in the core pod (see operator doc for more info).
In a non-containerized system, you must create the relevant file manually and place it in 'connections' config sub-directory.
Connect file contains a single json with the fields specified below.

### Common Connection Fields
- name: A string identifying the connection (mandatory).
- notification_protocol: One of: http, https, kafka (mandatory).

### Http(s) Connection Fields
- agent_request_object: Value is a JSON that is passed to to nodejs' http(s) agent (mandatory).
Any field supported by nodejs' http(s) agent can be used, specifically 'host' and 'port'.
A full list of options is [here](https://nodejs.org/docs/latest-v22.x/api/http.html#new-agentoptions).

- request_options_object: Value is a JSON that is passed to nodejs' http(s) request (optional).
Any field supported by nodejs' http(s) request option can be used, specifically:
- 'path': used to specify the url path
- 'auth': used for http simple auth. Value for 'auth' is of the syntax: <name>:<passowrd>.

A full list of options is [here](https://nodejs.org/docs/latest-v22.x/api/http.html#httprequesturl-options-callback).

### Kafka Connection Fields
- metadata.broker.list: A CSV list of Kafka brokers (mandatory).
- topic: A topic for the Kafka message (mandatory).

## Event Types
S3 spec lists several events and "sub events".

The list of supported events are:

- s3:ObjectCreated:*
- s3:ObjectCreated:Put
- s3:ObjectCreated:Post
- s3:ObjectCreated:Copy
- s3:ObjectCreated:CompleteMultipartUpload
- s3:ObjectRemoved:*
- s3:ObjectRemoved:Delete
- s3:ObjectRemoved:DeleteMarkerCreated
- s3:ObjectRestore:*
- s3:ObjectRestore:Post
- s3:ObjectRestore:Completed
- s3:ObjectRestore:Delete
- s3:ObjectTagging:*
- s3:ObjectTagging:Put
- s3:ObjectTagging:Delete
- s3:LifecycleExpiration:*
- s3:LifecycleExpiration:Delete
- s3:LifecycleExpiration:DeleteMarkerCreated

## Event Processing and Failure Handling
Once NooBaa finds an event with a relevant notification configuration, the notification
is written to a persistent file.
Location of persistent files is determined by-
- For containerized, the pvc specified in NooBaa Bucket Notification spec (see Operator docs for more info).
- For NC, the env variable NOTIFICATION_LOG_DIR (see NC docs for more info).

Files are processed by-
- For containerized, files are contantly being processed in the background of the core pod.
- For NC, files are processed by running manage_nsfs with 'notification' action.

If a notification fails to be sent to the external server, it will be re-written to a persistent file.
Once this new file is processed, NooBaa will try to re-send the failed notification.