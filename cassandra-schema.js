// -*- mode: js2; -*-
"use strict";


const P         = require('bluebird');
const cassandra = require('cassandra-driver');
const lookup    = P.promisify(require('dns').lookup);

const balancing = cassandra.policies.loadBalancing;


const SELECT_SCHEMA_PEERS = `SELECT peer,rpc_address,schema_version FROM system.peers`;
const SELECT_SCHEMA_LOCAL = `SELECT rpc_address,schema_version FROM system.local WHERE key='local'`;


/** Creates a single entry whitelisting loadbalancing policy. */
const whitelist = (contact) => {
    return new balancing.WhiteListPolicy(new balancing.RoundRobinPolicy(), [ contact ]);
};


/** Creates a single connection pool. */
function connect(host, port, options) {
    const disableSsl = (options && options.withoutSsl) ? true : false;
    const maxAgreementWait = (options && options.maxAgreementWait) ? options.maxAgreementWait : 10;
    const creds = options.credentials;
    return lookup(host)
        .then((address) => {
            const contact = `${address}:${port}`;
            const config = {
                contactPoints: [ contact ],
                authProvider: new cassandra.auth.PlainTextAuthProvider(creds.username, creds.password),
                promiseFactory: P.fromCallback,
                protocolOptions: { maxSchemaAgreementWait: maxAgreementWait },
                queryOptions: { consistency: cassandra.types.consistencies.one },
                policies: { loadBalancing: whitelist(contact) }
            };
            if (!disableSsl)
                config.sslOptions = { ca: '/dev/null' };
            const client = new cassandra.Client(config);
	    return client.connect().then(() => client);
        });
}


function getPeerVersions(host, port, options) {
    return connect(host, port, options)
        .then((client) => {
            return client.execute(SELECT_SCHEMA_PEERS)
                .then((res) => {
		    let peers = {};
                    // TODO: This should determine if the host is up or not, and ignore hosts that are down (if down,
                    // disagreement is to be expected).
		    res.rows.forEach((row) => {
			peers[row.rpc_address] = row.schema_version.toString();
		    });
		    return peers;
                })
		.finally(() => client.shutdown());
        });
}


function getLocalVersion(host, port, options) {
    return connect(host, port, options)
        .then((client) => {
            return client.execute(SELECT_SCHEMA_LOCAL)
                .then((res) => {
                    if (res.rows.length !== 1)
			throw new Error(`Failed to aquire local schema info from ${host}:${port}`);
		    const row = res.rows[0];
		    const local = {};
		    // XXX: Store the string representation here, because we'll later use a Set to determine the number
		    // of unique schema versions; Uuid objects for identical uuids aren't necessary equal according to
		    // Set's equality algorithm... Because Reasons (see:
		    // https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Set#Value_equality)
		    local[row.rpc_address] = row.schema_version.toString();
		    return local;
                })
		.finally(() => client.shutdown());
        });
}


function getVersions(host, port, options) {
    return getPeerVersions(host, port, options)
	.then((peers) => {
	    const result = Object.assign({}, peers);
	    return getLocalVersion(host, port, options)
		.then((local) => {
		    return Object.assign(result, local);
		});
	});
}


function checkSchemaAgreement(host, port, options) {
    return getVersions(host, port, options)
	.then((versions) => {
	    const values = (obj) => Object.keys(obj).map((o) => obj[o]);
	    const unique = new Set(values(versions));
	    return {
		agrees: unique.size === 1,
		versions
	    };
	});
}


module.exports = { checkSchemaAgreement, connect };
