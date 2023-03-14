/* Copyright (C) 2016 NooBaa */
'use strict';

const mocha = require('mocha');
const assert = require('assert');
const s3select_utils = require('../../util/s3select');
const stream = require('stream');
const nb_native = require('../../util/nb_native')();
const { Transform } = require('readable-stream');
const stream_utils = require('../../util/stream_utils');

const csv_small_str =
`a,b,c,d,e,
0,414,339,155,67,
1,60,741,755,698,
2,572,53,375,241,
3,966,233,168,371,
4,858,55,964,901,
5,630,524,728,489,
6,355,260,504,32,
7,636,736,662,50,
8,75,169,470,487,
9,910,225,185,834,
`;

const json_str =
`[
{"doublef": 3.3, "intf" : 3, "stringf": "i am a string"},
{"doublef": 3.3, "intf" : 123, "stringf": "i am a string"}
]
`;

class S3SelectStream extends Transform {

    constructor(context) {
        super({});
        this.s3select = new (nb_native.S3Select)(context);
    }

    async _transform(chunk, encoding, cb) {
        //console.log("got chunk ", chunk.length);
        let select = await this.s3select.write(chunk);
        if (select) {
            this.push(select.select);
        }
        return cb();
    }

    async _flush(cb) {
        //console.log("in flush");
        let select = await this.s3select.flush();
        if (select) {
            this.push(select.select);
        }
        return cb();
    }
}

class CollectChunks extends stream.Writable {

    chunks = [];

    _write(chunk, enc, next) {
        //console.log("got chunk");
        this.chunks.push(chunk);
        next();
    }

}

class FailingTransform extends Transform {

    constructor(writable) {
        super({});
        this.writable = writable;
    }

    _transform(chunk, enconding, cb) {
        this.writable.write(chunk);
        return cb();
    }

    _flush(cb) {
        if (this.writable.flush) {
            this.writable.flush();
        }
        return cb();
    }

}

/*class FailingReadble extends Readable {

}*/

async function run_sql(args, input_str) {
    const s3select = new S3SelectStream(args);
    const input_stream = stream.Readable.from([input_str]);
    const collect = new CollectChunks();
    await stream.promises.pipeline(input_stream, s3select, collect);
    return Buffer.concat(collect.chunks).toString();
}

async function simulate_async_loop(writable) {
    writable.write("this is an ");
    await new Promise(resolve => {
        writable.write("input string.", null, () => {
            resolve();
        });
    });
    assert(writable.emit('error', new Error("I am an error event in an async loop.")));
}


mocha.describe('s3select', function() {

    mocha.it('should create ok', function() {
        const select_args = {
            query: "select sum(int(_2)) from stdin;",
            input_format: "CSV",
            input_serialization_format: {"FieldDelimiter": ",", "RecordDelimiter": "\n"},
            records_header_buf: s3select_utils.S3SelectStream.records_message_headers
        };
        const s3select = new s3select_utils.S3SelectStream(select_args);
        s3select.destroy();
    });

    mocha.it('select star', async function() {
        const select_args = {
            query: "select * from stdin;",
            input_format: "CSV",
            input_serialization_format: {"FieldDelimiter": ",", "RecordDelimiter": "\n"},
            records_header_buf: s3select_utils.S3SelectStream.records_message_headers
        };
        const output = await run_sql(select_args, csv_small_str);
        assert.strictEqual(output, csv_small_str);
    });

    mocha.it('select sum with where', async function() {
        const select_args = {
            query: "select sum(int(_2)) from stdin where int(_1) % 3 = 1;",
            input_format: "CSV",
            input_serialization_format: {"FieldDelimiter": ",", "RecordDelimiter": "\n"},
            records_header_buf: s3select_utils.S3SelectStream.records_message_headers
        };
        const output = await run_sql(select_args, csv_small_str);
        assert.strictEqual(output, "1554");
    });


    mocha.it('select with header name', async function() {
        const select_args = {
            query: "select c from stdin where int(a) < 5;",
            input_format: "CSV",
            input_serialization_format: {"FieldDelimiter": ",", "RecordDelimiter": "\n", "FileHeaderInfo": "USE"},
            records_header_buf: s3select_utils.S3SelectStream.records_message_headers
        };
        const output = await run_sql(select_args, csv_small_str);
        assert.strictEqual(output, "339\n741\n53\n233\n55\n");
    });

    mocha.it('select json', async function() {
        const select_args = {
            query: "select _1.intf from S3Object[*] s;",
            input_format: "JSON",
            input_serialization_format: {},
            records_header_buf: s3select_utils.S3SelectStream.records_message_headers
        };
        const output = await run_sql(select_args, json_str);
        assert.strictEqual(output, "3\n123\n");
    });

    mocha.it('double pipe close', async function() {
        const readable = stream.Readable.from(["this is an input string."]);
        const collect = new CollectChunks();
        const transform = new FailingTransform(collect);
        stream_utils.pipeline([transform, collect], true /*res is a write stream, no need for resume*/);
        await stream_utils.pipeline([readable, transform], true);
        const output = Buffer.concat(collect.chunks).toString();
        assert.strictEqual(output, "this is an input string.");
        assert(readable.emit('error', new Error('readale error')));
        assert(collect.closed);
    });

    mocha.it('async loop error', async function() {
        const collect = new CollectChunks();
        const transform = new FailingTransform(collect);
        stream_utils.pipeline([transform, collect], true /*res is a write stream, no need for resume*/);
        await simulate_async_loop(transform);
        const output = Buffer.concat(collect.chunks).toString();
        assert.strictEqual(output, "this is an input string.");
        assert(collect.closed);
    });
});
