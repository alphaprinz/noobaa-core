/* Copyright (C) 2022 NooBaa */
'use strict';

// use the replacer parameter of JSON.stringify to serialize BigInt values
const replacer = (key, value) => key === "ts" ? value.toString() : value;

// newTimeline is a factory function which returns an instance of
// a Timeline object, used to measure latency of long running flows
// and latency of each sub phase
// a timestamp named 'start' is recorded during object construction
// a timestamp named 'end' is recorded during timeline report
function newTimeline(name) {
    const tl = {
        name, // name of this timeline
        line: [], // array of recorded timestamp

        // record a named timestamp
        timestamp: function(name) {
            this.line.push(
                { name,
                  ts: process.hrtime.bigint(),
                }
            );
        },

        // print an arbitrary representation of the timeline
        report: function() {
            this.timestamp('end');
            const start = this.line[0].ts;
            const end = this.line[this.line.length - 1].ts;
            const took = end - start;
            const normalize_by_start = (e) => ({name: e.name, ts: e.ts - start});
            const out_obj = {
                name: this.name,
                line: this.line.map(normalize_by_start)
            };
            console.log("Timeline", this.name, "took", took, "JSON", JSON.stringify(out_obj, replacer));
        },
    };
    tl.timestamp("start");
    return tl;
}

module.exports = newTimeline;
