'use strict';

var shareIP = process.env.DB_IP || process.env.IP ||'127.0.0.1' ||'203.162.13.170'  || '139.162.22.62' ;

var dbcfg = {
	n1ql: `${shareIP}:8093`,
	clusterUrl: `couchbase://${shareIP}:8091`,
	dbName: 'default'
};

var couchbase = require('couchbase');
var cluster = new couchbase.Cluster(dbcfg.clusterUrl);
var bucket = cluster.openBucket(dbcfg.dbName);
bucket.enableN1ql([dbcfg.n1ql]);
bucket.operationTimeout = 240 * 1000;
var N1qlQuery = require('couchbase').N1qlQuery;

console.log("Call import mydb", dbcfg.clusterUrl);

let commonModel = {
	upsert(dto,callback) {
		bucket.upsert(dto.id, dto, function (err, res) {
			if (err) {
				console.log("ERROR:" + err);
			}
			if(callback)
				callback(err,res);
		})
	},

	insert(dto,callback) {
		bucket.insert(dto.id, dto, function (err, res) {
			if (err) {
				console.log("ERROR:" + err);
			}
			if(callback)
				callback(err,res);
		})
	},

	query(sql, callback) {
		console.log("Call query:",sql);

		var query = N1qlQuery.fromString(sql);
		query.timeout = 3600;
		bucket.query(query, callback);
	},

	byId(id, callback) {
		let sql  = `select t.* from default t where id ='${id}'`;

		var query = N1qlQuery.fromString(sql);
		bucket.query(query, (err ,list) => {
			if (err) {
				console.error("Error:", err);
				callback(err, null);
				return null;
			}

			if (!list || list.length==0) {
				callback(null, null);
				return null;
			}

			callback(null, list[0]);
		});
	},

	countByType(type, onSuccess) {
		var sql = `select count(*) cnt from default where type = '${type}'`;
		var query = N1qlQuery.fromString(sql);

		bucket.operationTimeout = 1200 * 1000;

		bucket.query(query, function (err, res) {
			if (err) {
				console.log('query failed'.red, err);
				return;
			}
			console.log('success!', res);

			onSuccess(res[0].cnt);
		});
	}

};

module.exports = commonModel;