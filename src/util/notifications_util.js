/* Copyright (C) 2024 NooBaa */
'use strict';

const dbg = require('../util/debug_module')(__filename);
const system_store = require('../server/system_services/system_store').get_instance();
const system_utils = require('../server/utils/system_utils');
const config = require('../../config');
const { PersistentLogger, LogFile } = require('../util/persistent_logger');
const Kafka = require('node-rdkafka');
const os = require('os');
const fs = require('fs');
const http = require('http');
const https = require('https');
const { get_process_fs_context } = require('./native_fs_utils');
const nb_native = require('../util/nb_native');
const http_utils = require('../util/http_utils');

class Notificator {

    /**
     *
     * @param {Object} options
     */

    constructor({name, fs_context}) {
        this.name = name;
        this.connect_str_to_connection = new Map();
        this.notif_to_connect = new Map();
        this.fs_context = fs_context ?? get_process_fs_context();
    }

    async run_batch() {
        if (!this._can_run()) return;
        dbg.log0('Notificator', this.name, " is starting.");
        try {
            await this.process_notification_files();
        } catch (err) {
            dbg.error('Notificator failure:', err);
        }

        return 100; //TODO
    }

    _can_run() {

        if (!system_store.is_finished_initial_load) {
            dbg.log0('system_store did not finish initial load');
            return false;
        }
        const system = system_store.data.systems[0];
        if (!system || system_utils.system_in_maintenance(system._id)) return false;

        return true;
    }

    /**
     * This function will process the persistent log of bucket logging
     * and will send its notifications
     */
    async process_notification_files() {
        const seen_nodes = new Set();
        const entries = await nb_native().fs.readdir(this.fs_context, config.NOTIFICATION_LOG_DIR);
        for(const file of entries) {
            dbg.log0("file.name = ", file.name);
            if (!file.name.endsWith('.log')) return;
            //get namespace
            const namepsace_index = file.name.indexOf(config.NOTIFICATION_LOG_NS);
            if (namepsace_index === -1) continue;
            const node_namespace = file.name.substring(0, namepsace_index + config.NOTIFICATION_LOG_NS.length);
            dbg.log0('namespace = ', node_namespace);
            if (seen_nodes.has(node_namespace)) {
                //already handled this node name
                continue;
            } else {
                seen_nodes.add(node_namespace);
            }
            const log = new PersistentLogger(config.NOTIFICATION_LOG_DIR, node_namespace, { locking: 'EXCLUSIVE' });
            try {
                await log.process(async (file, failure_append) => this._notify(this.fs_context, file, failure_append));
            } catch (err) {
                dbg.error('processing notifications log file failed', log.file);
                throw err;
            } finally {
                await log.close();
            }
        }
    }

    /**
     * @param {nb.NativeFSContext} fs_context
     * @param {string} log_file
     * @returns {Promise<Boolean>}
     */
    async _notify(fs_context, log_file, failure_append) {
        const file = new LogFile(fs_context, log_file);
        dbg.log0('sending out notifications from file ', log_file);
        const send_promises = [];
        await file.collect_and_process(async str => {
            dbg.log0("collect_and_process str = ", str);
            const notif = JSON.parse(str);
            let connect = this.notif_to_connect.get(notif.meta.name);
            if (!connect) {
                connect = parse_connect_file(notif.meta.connect);
                this.notif_to_connect.set(notif.meta.name, connect);
            }
            let connection = this.connect_str_to_connection.get(notif.meta.name);
            if (!connection) {
                let promise_failure_cb = async (err) => {
                    dbg.error("failed to send notification", err);
                    await failure_append(str);
                };
                connection = get_connection(connect, promise_failure_cb);
                try{
                    await connection.connect();
                } catch(err) {
                    //failed to connect
                    dbg.error("Connection failed for", connect);
                    await failure_append(str);
                    return;
                }
                this.connect_str_to_connection.set(notif.meta.name, connection);
            }

            const send_promise = connection.promise_notify(notif);
            if (send_promise) send_promises.push(send_promise);
        });
        //note we can't reject promises here, since Promise.all() is rejected on
        //first rejected promise, and that would not await other send_promises()
        await Promise.all(send_promises);
        //as failed sends are written to failure log,
        //we can remove the currently processed persistent file
        return true;
    }
}

class HttpNotificator {

    constructor(connect_obj, promise_failure_cb) {
        this.connect_obj = connect_obj;
        this.promise_failure_cb = promise_failure_cb;
        this.protocol = connect_obj.notification_protocol.toLowerCase() === 'https' ? https : http;
    }

    connect() {
        this.agent = new this.protocol.Agent(this.connect_obj.agent_request_object);
    }

    promise_notify(notif) {
        return new Promise(resolve => {
            const req = this.protocol.request({
                agent: this.agent,
                method: 'POST',
                ...this.connect_obj.request_options_object},
                result => {
                //result.pipe(process.stdout);
                resolve();
            });
            req.on('error', err => {
                if (req.destroyed) {
                    //error emitted because of timeout, nothing more to do
                    return;
                }
                this.promise_failure_cb(err).then(resolve);
            });
            req.on('timeout', () => {
                req.destroy();
                this.promise_failure_cb('timeout for ' + JSON.stringify(notif)).then(resolve);
            }); 
            req.write(JSON.stringify(notif.notif));
            req.end();
        });
    }

    destroy() {
        this.agent.destroy();
    }
}

class KafkaNotificator {

    constructor(connect_obj, promise_failure_cb) {
        this.connect_obj = connect_obj;
        this.promise_failure_cb = promise_failure_cb;
    }

    async connect() {
        //kafka client doens't like options it's not familiar with
        //so delete them before connecting
        const connect_for_kafka = structuredClone(this.connect_obj);
        delete connect_for_kafka.topic;
        delete connect_for_kafka.notification_protocol;
        delete connect_for_kafka.name;
        this.connection = new Kafka.HighLevelProducer(connect_for_kafka);
        await new Promise((res, rej) => {
            this.connection.on('ready', () => {
                console.log("connect ready");
                res();
            });
            this.connection.on('connection.failure', (err) => {
                console.log("connect fail");
                rej(err);
            });
            this.connection.on('event.log', (arg) => {
                console.log("event log", arg);
            });
            this.connection.connect();
        });
        this.connection.setPollInterval(100);
    }

    promise_notify(notif) {
        const connect_obj = this.connect_obj;
        return new Promise(resolve => {
            this.connection.produce(
                connect_obj.topic,
                null,
                Buffer.from(JSON.stringify(notif.notif)),
                null,
                Date.now(),
                (err, offset) => {
                    if (err) {
                        this.promise_failure_cb(err).then(resolve);
                    } else {
                        resolve();
                    }
                }
            )
        });
    }

    destroy() {
        this.connection.flush(10000);
        this.connection.disconnect();
    }
}

//replace properties starting with 'local_file'
//with the content of the pointed file
//(useful for loading tls certificates into http options object)
function load_files(object) {
    dbg.log0('load_files for obj =', object);
    if (typeof object !== 'object') return;
    for (const key in object) {
        if(key.startsWith('local_file')) {
            const new_key = key.substring('local_file_'.length);
            const content = fs.readFileSync(object[key], 'utf-8');
            object[new_key] = content;
            delete object[key];
        } else {
            load_files(object[key]);
        }
    }
    dbg.log0('load_files done new obj =', object);
}

function parse_connect_file(connect_filepath) {
    const connect = {};
    const connect_strs = fs.readFileSync(connect_filepath, 'utf-8').split(os.EOL);
    for (const connect_str of connect_strs) {
        if(connect_str === '') continue;
        const kv = connect_str.split('=');
        //parse JSONs-
        if (kv[0].endsWith('object')) {
            kv[1] = JSON.parse(kv[1]);
        }
        connect[kv[0]] = kv[1];
    }
    //parse file contents (useful for tls cert files)
    load_files(connect);
    return connect;
}


function get_connection(connect, promise_failure_cb) {
    switch (connect.notification_protocol.toLowerCase()) {
        case 'http':
        case 'https':
        {
            return new HttpNotificator(connect, promise_failure_cb);
        }
        case 'kafka': {
            return new KafkaNotificator(connect, promise_failure_cb);
        }
        default: {
            dbg.error("Unknown notification protocol", connect.notification_protocol);
            //nothing more to possibly do with this notification, don't append to failure log
            return;
        }
    }
}


async function test_notifications(bucket) {
    for (const notif of bucket.notifications) {
        const connect = parse_connect_file(notif.connect);
        dbg.log1("testing notif", notif);
        try {
            const connection = get_connection(connect, async err => {return err;});
            await connection.connect();
            await connection.promise_notify({notif:"test notification"});
            connection.destroy();
        } catch(err) {
            dbg.error("Connection failed for", connect);
            return err;
        }
    }
}

//see https://docs.aws.amazon.com/AmazonS3/latest/userguide/notification-content-structure.html
function compose_notification(req, res, bucket, notif_conf) {
    let eTag = res.getHeader('ETag');
    if (eTag && eTag.startsWith('\"') && eTag.endsWith('\"')) {
        eTag = eTag.substring(2, eTag.length - 2);
    }

    const notif = {
        eventVersion: '2.3',
        eventSource: req.system ? req.system.name : system_store.data.systems[0]._id + ':s3',
        eventTime: new Date().toISOString,
        eventName: req.s3event + ':' + (req.s3_event_op || req.method),
        userIdentity: {
            principalId: req.object_sdk.requesting_account.name,
        },
        requestParameters: {
            sourceIPAddress: http_utils.parse_client_ip(req),
        },
        responseElements: {
            "x-amz-request-id": req.request_id,
            "x-amz-id-2": req.request_id,
        },
        s3: {
            s3SchemaVersion: "1.0",
            configurationId: notif_conf.name,
            bucket: {
                name: bucket.name,
                ownerIdentity: {
                    principalId: bucket.bucket_owner.unwrap(),
                },
                arn: "arn:aws:s3:::" + bucket.name,
            },
            object: {
                key: req.params.key,
                size: res.getHeader('content-length'),
                eTag,
                versionId: res.getHeader('x-amz-version-id'),
            },
        }
    };

    //handle glacierEventData
    if (res.restore_object_result) {
        notif.glacierEventData = {
            restoreEventData: {
                lifecycleRestorationExpiryTime: res.restore_object_result.expires_on.toISOString(),
                lifecycleRestoreStorageClass: res.restore_object_result.storage_class,
            },
        }
    }

    //handle sequencer
    if (res.seq) {
        notif.s3.object.sequencer = res.seq;
    }

    const records = [];
    records.push(notif);
    
    return {Records: records}
}

exports.Notificator = Notificator;
exports.test_notifications = test_notifications;
exports.compose_notification = compose_notification;
