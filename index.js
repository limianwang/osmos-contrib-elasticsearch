'use strict';

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
  process.nextTick(function() {
    callback(null, {});
  });
};

Driver.prototype.get = function get(model, key, callback) {
  return this.client.get(
    {
      index: this.index,
      type: model.bucket,
      id: key
    },

    function(err, result) {
      if(err) {
        callback(err);
      } else {
        result._source[model.schema.primaryKey] = result._id;

        callback(null, result._source);
      }
    }
  );
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

  this.client.index(
    payload,

    function(err, result) {
      if (err) {
        callback(err);
        return;
      }

      document.primaryKey = result._id;
      callback(null);
    }
  );
};

Driver.prototype.put = function put(document, set, unset, callback) {
  var self = this;

  process.nextTick(function() {
    if (!document.model.schema.primaryKey || !document.primaryKey) {
      throw new Error('You cannot put a document without a primary key');
    }

    if (document.__originalData__[document.model.schema.primaryKey] === undefined) {
      return self.post(document, data, callback);
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
      index: self.index,
      type: document.model.bucket,
      consistency: 'quorum',
      refresh: true,
      body: {
        doc: diff
      },
      id: primaryKey
    };

    self.client.update(payload, callback);
  });
};

Driver.prototype.del = function deleteRecord(model, key, callback) {
  if (key.constructor.name === 'Object') {
    key = key[Object.keys(key)[0]];
  }

  return this.client.delete(
    {
      index: this.index,
      type: model.bucket,
      id: key,
      refresh: true
    },
    function(err) {
      callback(err);
    }
  );
};

Driver.prototype.findOne = function findOne(model, spec, callback) {
  var body = merge({ size: 1 }, spec);

  var query = {
    index: this.index,
    type: model.bucket,
    body: body
  };

  return this.client.search(query, function(err, result) {
    if (err) {
      callback(err);
      return;
    }

    var records = result.hits.hits;

    records = records.map(function(record) {
      record._source[model.schema.primaryKey] = record._id;

      return record._source;
    });

    if (records.length) {
      callback(null, records[0]);
    } else {
      callback(null, null);
    }
  });
};

Driver.prototype.count = function count(model, spec, callback) {
  return this.find(model, spec, function(err, records) {
    if(err) {
      callback(err);
    } else {
      callback(null, records.length);
    }
  });
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

  return this.client.search(query, function(err, result) {
    if (err) {
      callback(err, []);
      return;
    }

    var records = result.hits.hits;

    records = records.map(function(record) {
      record._source[model.schema.primaryKey] = record._id;

      return record._source;
    });

    callback(null, records);
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

  return this.client.search(query, function(err, result) {
    if (err) {
      callback(err, []);
    } else {
      var records = result.hits.hits;

      records = records.map(function(record) {
        record._source[model.schema.primaryKey] = record._id;

        return record._source;
      });

      callback(
        null,
        {
          docs: records,
          count: result.hits.total,
          start: start,
          limit: limit
        }
      );
    }
  });
};

module.exports = Driver;
