/* Copyright (C) 2016 NooBaa */
'use strict';

/**
 * http://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketGETnotification.html
 */
async function get_bucket_notification(req) {

    const result = await req.object_sdk.get_bucket_notification({
        bucket_name: req.params.bucket,
    });

    //adapt to aws cli structure
    if (result && result.length > 0) {
        for (const conf of result) {
            conf.Event = conf.event;
            conf.Topic = conf.topic;
            conf.Id = conf.id;
            delete conf.vent;
            delete conf.topic;
            delete conf.id;
        }
    }

    const reply = result && result.length > 0 ?
        {
            //return result inside TopicConfiguration tag
            NotificationConfiguration: {
                TopicConfiguration: result
            }
        } :
        //if there's no notification, return empty NotificationConfiguration tag
        { NotificationConfiguration: {} };


    return reply;
}

module.exports = {
    handler: get_bucket_notification,
    body: {
        type: 'empty',
    },
    reply: {
        type: 'xml',
    },
};
