'use strict';

var Promise = require('native-or-bluebird');
var es = require('elasticsearch');
var merge = require('node-helper-utilities').merge;

var Driver = function OsmosElasticDriver(options, index) {
  this.client = es.Client(options);
  this.index = index;
};

Driver.prototype.createIndices = function createIndex(model, data, callback) {
  if('object' !== typeof data) {
    throw new Error('`data` needs to be a valid object!');
  }

  if(!data.hasOwnProperty('type')) {
    data.type = model.bucket;
  }

  if(!data.hasOwnProperty('body')) {
    data.body = {};
  }

  var indices = merge(data, {
    index: this.index
  });

  return this.client.indices.create(indices, function(err) {
    callback(err);
  });
};

Driver.prototype.create = function create(model, callback) {
  return Promise.resolve().nodeify(callback);
};

Driver.prototype.get = function get(model, key, callback) {
  var spec = {
    index: this.index,
    type: model.bucket,
    id: key
  };
  return this.client.get(spec).then(function(result) {
    result._source[model.schema.primaryKey] = result._id;

    return Promise.resolve(result._source);
  }).nodeify(callback);
};

Driver.prototype.post = function post(document, data, callback) {
  var payload = {
    index: this.index,
    type: document.model.bucket,
    body: data,
    published: true,
    refresh: true,
    consistency: 'quorum'
  };

  if (document.primaryKey) {
    payload.id = document.primaryKey;
  }

  return this.client.index(payload).then(function(result) {
    document.primaryKey = result._id;

    return Promise.resolve();
  }).nodeify(callback);
};

Driver.prototype.put = function put(document, set, unset, callback) {
  if(!(document.model.schema.primaryKey && document.primaryKey)) {
    return Promise.reject(new Error('You cannot put a document without a primary key')).nodeify(callback);
  }

  if(!document.__originalData__[document.model.schema.primaryKey]) {
    return this.post(document, data).nodeify(callback);
  }

  var diff = {};
  Object.keys(set).forEach(function(k) {

    diff[k] = set[k];
  });

  Object.keys(unset).forEach(function(k) {
    diff[k] = null;
  });

  var primaryKey;

  if (diff[document.schema.primaryKey]) {
    primaryKey = document.__originalData__[document.schema.primaryKey];
  } else {
    primaryKey = document.primaryKey;
  }

  var payload = {
    index: this.index,
    type: document.model.bucket,
    consistency: 'quorum',
    refresh: true,
    body: {
      doc: diff
    },
    id: primaryKey
  };

  return this.client.update(payload);
};

Driver.prototype.del = function deleteRecord(model, key, callback) {
  if (key.constructor.name === 'Object') {
    key = key[Object.keys(key)[0]];
  }
  var spec ={
    index: this.index,
    type: model.bucket,
    id: key,
    refresh: true
  };

  return this.client.delete(spec);
};

Driver.prototype.findOne = function findOne(model, spec, callback) {
  var body = merge({ size: 1 }, spec);

  var query = {
    index: this.index,
    type: model.bucket,
    body: body
  };

  return this.client.search(query).then(function(result) {
    var records = result.hits.hits;

    records = records.map(function(record) {
      record._source[model.schema.primaryKey] = record._id;

      return record._source;
    });

    if (records.length) {
      return Promise.resolve(records[0]);
    } else {
      return Promise.resolve();
    }
  }).nodeify(callback);
};

Driver.prototype.count = function count(model, spec, callback) {
  return this.find(model, spec).then(function(records) {
    return Promise.resolve(records.length);
  }).nodeify(callback);
};

Driver.prototype.find = function find(model, spec, callback) {
  var body = merge({
    size: 9999999
  }, spec);

  var query = {
    index: this.index,
    type: model.bucket,
    body: body
  };

  return this.client.search(query).then(function(result) {
    var records = result.hits.hits;

    records = records.map(function(record) {
      record._source[model.schema.primaryKey] = record._id;

      return record._source;
    });

    return Promise.resolve(records);
  });
};

Driver.prototype.findLimit = function findLimit(model, spec, start, limit, callback) {
  var body = merge({
    from: start,
    size: limit
  }, spec);

  var query = {
    index: this.index,
    type: model.bucket,
    body: body
  };

  return this.client.search(query).then(function(err, result) {
    var records = result.hits.hits;

    records = records.map(function(record) {
      record._source[model.schema.primaryKey] = record._id;

      return record._source;
    });

    return Promise.resolve({
      docs: records,
      count: result.hits.total,
      start: start,
      limit: limit
    });
  });
};

module.exports = Driver;
