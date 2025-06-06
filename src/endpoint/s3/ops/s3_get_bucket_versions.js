/* Copyright (C) 2016 NooBaa */
'use strict';

const _ = require('lodash');
const dbg = require('../../../util/debug_module')(__filename);
const S3Error = require('../s3_errors').S3Error;
const s3_utils = require('../s3_utils');

/**
 * http://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketGETVersion.html
 */
async function get_bucket_versions(req) {

    const max_keys_received = Number(req.query['max-keys'] || 1000);
    if (!Number.isInteger(max_keys_received) || max_keys_received < 0) {
        dbg.warn('Invalid max-keys', req.query['max-keys']);
        throw new S3Error(S3Error.InvalidArgument);
    }

    if (req.query['version-id-marker'] && !req.query['key-marker']) {
        dbg.warn('A version-id marker cannot be specified without a key marker');
        throw new S3Error(S3Error.InvalidArgument);
    }
    const version_id_marker = s3_utils.parse_version_id(req.query['version-id-marker'], S3Error.InvalidArgumentEmptyVersionIdMarker);
    const reply = await req.object_sdk.list_object_versions({
        bucket: req.params.bucket,
        prefix: req.query.prefix,
        delimiter: req.query.delimiter,
        key_marker: req.query['key-marker'],
        version_id_marker,
        limit: Math.min(max_keys_received, 1000),
    });

    const field_encoder = s3_utils.get_response_field_encoder(req);
    const default_object_owner = await s3_utils.get_default_object_owner(req.params.bucket, req.object_sdk);

    return {
        ListVersionsResult: [{
            Name: req.params.bucket,
            Prefix: field_encoder(req.query.prefix) || '',
            Delimiter: field_encoder(req.query.delimiter),
            MaxKeys: max_keys_received,
            KeyMarker: field_encoder(req.query['key-marker']) || '',
            VersionIdMarker: version_id_marker || '',
            IsTruncated: reply.is_truncated,
            NextKeyMarker: field_encoder(reply.next_marker),
            NextVersionIdMarker: reply.next_version_id_marker,
            EncodingType: req.query['encoding-type'],
        },
        _.map(reply.objects, obj => (obj.delete_marker ? ({
            DeleteMarker: {
                Key: field_encoder(obj.key),
                VersionId: obj.version_id || 'null',
                IsLatest: obj.is_latest,
                LastModified: s3_utils.format_s3_xml_date(obj.create_time),
                Owner: s3_utils.get_object_owner(obj) || default_object_owner,
            }
        }) : ({
            Version: {
                Key: field_encoder(obj.key),
                VersionId: obj.version_id || 'null',
                IsLatest: obj.is_latest,
                LastModified: s3_utils.format_s3_xml_date(obj.create_time),
                ETag: `"${obj.etag}"`,
                Size: obj.size,
                Owner: s3_utils.get_object_owner(obj) || default_object_owner,
                StorageClass: s3_utils.parse_storage_class(obj.storage_class),
            }
        }))),
        _.map(reply.common_prefixes, prefix => ({
            CommonPrefixes: {
                Prefix: field_encoder(prefix) || ''
            }
        }))
        ]
    };
}

module.exports = {
    handler: get_bucket_versions,
    body: {
        type: 'empty',
    },
    reply: {
        type: 'xml',
    },
};
