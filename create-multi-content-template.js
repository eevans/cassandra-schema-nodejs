// -*- mode: js2; -*-
"use strict";

/*
 * Sample script to create Cassandra schema in the YAML format
 * expected by the `mkschema` script.  Copy and modify as necessary.
 */


const crypto    = require('crypto');
const P         = require('bluebird');


function hashKey(key) {
    return new crypto.Hash('sha1')
        .update(key)
        .digest()
        .toString('base64')
        // Replace [+/] from base64 with _ (illegal in Cassandra)
        .replace(/[+/]/g, '_')
        // Remove base64 padding, has no entropy
        .replace(/=+$/, '');
}


function getValidPrefix(key) {
    const prefixMatch = /^[a-zA-Z0-9_]+/.exec(key);
    if (prefixMatch) {
        return prefixMatch[0];
    } else {
        return '';
    }
}


function makeValidKey(key, length) {
    const origKey = key;
    key = key.replace(/_/g, '__')
                .replace(/\./g, '_');
    if (!/^[a-zA-Z0-9_]+$/.test(key)) {
        // Create a new 28 char prefix
        const validPrefix = getValidPrefix(key).substr(0, length * 2 / 3);
        return validPrefix + hashKey(origKey).substr(0, length - validPrefix.length);
    } else if (key.length > length) {
        return key.substr(0, length * 2 / 3) + hashKey(origKey).substr(0, length / 3);
    } else {
        return key;
    }
}


function keyspaceName(name, table) {
    const reversedName = name.toLowerCase().split('.').reverse().join('.');
    const prefix = makeValidKey(reversedName, Math.max(26, 48 - table.length - 3));
    // 6 chars _hash_ to prevent conflicts between domains & table names
    const res = `${prefix}_T_${makeValidKey(table, 48 - prefix.length - 3)}`;
    return res;
}


function cassID(name) {
    if (/^[a-zA-Z0-9_]+$/.test(name)) {
        return `"${name}"`;
    } else {
        return `"${name.replace(/"/g, '""')}"`;
    }
}


const tables = {
    'parsoid_ng.data-parsoid': 'text',
    'parsoid_ng.html': 'blob',
    'parsoid_ng-render-timeline': '',
    'parsoid_ng-revision-timeline': '',
    'parsoid_ng.section-offsets': 'text'
};

const storages = [
    'enwiki',
    'commons',
    'wikipedia',
    'others'
];

const qKs = `CREATE KEYSPACE IF NOT EXISTS <keyspace> WITH replication = {'class': 'NetworkTopologyStrategy', 'codfw': '3', 'eqiad': '3'}  AND durable_writes = true;`;

const qMeta = `CREATE TABLE IF NOT EXISTS <keyspace>.meta (
    key text PRIMARY KEY,
    value text
) WITH bloom_filter_fp_chance = 0.1
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';`;

const qData = `CREATE TABLE IF NOT EXISTS <keyspace>.data (
    "_domain" text,
    key text,
    rev int,
    tid timeuuid,
    "content-location" text,
    "content-type" text,
    tags set<text>,
    value <type>,
    PRIMARY KEY (("_domain", key), rev, tid)
) WITH CLUSTERING ORDER BY (rev DESC, tid DESC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 86400
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';`;

const qDataRevision = `CREATE TABLE IF NOT EXISTS <keyspace>.data (
    "_domain" text,
    key text,
    ts timestamp,
    rev int,
    PRIMARY KEY (("_domain", key), ts)
) WITH CLUSTERING ORDER BY (ts DESC)
    AND bloom_filter_fp_chance = 0.1
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 864000
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';`;

const qDataRender = `CREATE TABLE IF NOT EXISTS <keyspace>.data (
    "_domain" text,
    key text,
    rev int,
    ts timestamp,
    tid timeuuid,
    PRIMARY KEY (("_domain", key), rev, ts)
) WITH CLUSTERING ORDER BY (rev DESC, ts DESC)
    AND bloom_filter_fp_chance = 0.1
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 864000
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';`;


return P.each(storages, (storage) => {
    return P.each(Object.keys(tables), (table) => {
        const queryf = (query, keyspace, type) => {
            let output = query.replace('<keyspace>', keyspace);
            if (type)
                output = output.replace('<type>', type);
            output.split('\n').forEach((line) => {
                console.log(`        ${line}`);
            });
        };
        const scrub = (s) => s.replace(/"/g, '');

        const keyspace = cassID(keyspaceName(storage, table));
        console.log(`create_${scrub(keyspace)}:`);
        console.log(`  statement: |`);
        queryf(qKs, keyspace);

        console.log(`create_${scrub(keyspace)}_meta:`);
        console.log(`  statement: |`);
        queryf(qMeta, keyspace);

        let dataCQL = qData;
        if(/render/.test(table)) {
            dataCQL = qDataRender;
        } else if(/revision/.test(table)) {
            dataCQL = qDataRevision;
        }

        console.log(`create_${scrub(keyspace)}_data:`);
        console.log(`  statement: |`);
        queryf(dataCQL, keyspace, tables[table]);
    });
});
