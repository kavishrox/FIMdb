var uuid = require('node-uuid');
var Promise = require('bluebird');
//fast in memory database
var logger, config, defaults, fbUtils;

var tableConfig = {};
var tables = {};

function parseConfig(options) {
  fbUtils = options.fbUtils;
  logger = options.logger;
  config = options.config;
  defaults = options.defaults;
}

function parseTableConfig(data) {
  for(var name in data) {
    tableConfig[name] = data[name];
  }
}

function parseTables(data) {
  for(var name in data) {
    if(tableConfig[name] && tableConfig[name].isPersistent) {
      tables[name] = data[name];
    } else {
      //remove this data --- later date
    }
  }
}

module.exports = {

  init: function(options) {
    //get everything stored on firebase
    if(!fbUtils) {
      parseConfig(options);
    }
    return new Promise(function(resolve, reject) {
      fbUtils.fetch({
        base: "fbFimConfig",
        path: "/"
      }).then(function(response) {
        parseTableConfig(response.data);
        fbUtils.fetch({
          base: "fbFimStore",
          path: "/"
        }).then(function(response) {
          parseTables(response.data);
          resolve({
            code : "200",
            message: "db initialized"
          });
        }, function(error) {
          reject(error);
        });
      }, function(error) {
        reject(error);
      });
    });
  },

  createTable: function(options) {
    var name = options.name;
    var fields = options.fields;
    var tableId = uuid.v4();
    var isPersistent = options.isPersistent || false;
    return new Promise(function(resolve, reject) {
      if(tableConfig[name]) {
        reject({
          code: "400",
          error: "Table already exists"
        });
      } else {
        fbUtils.update({
          base: "fbFIMConfig",
          path: "/"+name,
          data: {
            id: tableId,
            isPersistent: isPersistent,
            fields: fields
          }
        }).then(function(response) {
          tableConfig[name] = {
            id: tableId,
            path: "/"+name,
            fields : fields
          };
          tables[table] = {};
          resolve({
            code : "200",
            message: "Table created",
            data: {
              name: name,
              id: tableId
            }
          });
        }, function(error) {
          reject(error);
        });
      }
    });
  },

  fetch: function(options) {
    var table = options.table;
    var key = options.key;
    return new Promise(function(resolve, reject) {
      if(tables[table]) {
        var curr = tables[table];
        for(var idx=0; idx<key.length; idx++) {
          if(curr[key[idx]]) {
            curr = curr[key[idx]];
          } else {
            curr = undefined;
            break;
          }
        }
        resolve({
          code :"200",
          message: "Value returned",
          data: curr
        });
      } else {
        reject({
          code : "400",
          error: "Table doesnt exist"
        });
      }
    });
  },
  
  store: function(options) {
    var table = options.table;
    var key = options.key;
    var value = options.value;
    return new Promise(function(resolve, reject) {
      if(tables[table]) {
        var oldValue = tables[table].key;
        tables[table].key = value;
        var data = {

        };
        data[key] = value;
        fbUtils.update({
          base: "fbFimStore",
          path: "/"+table,
          data: data
        }).then(function(response) {
          resolve(response);
        }, function(error) {
          //remove the local data
          if(oldValue) {
            tables[table].key = oldValue;
          } else {
            delete tables[table].key;
          }
          reject(error);
        });
      } else {
        reject({
          code: "400",
          error: "Table doesnt exist"
        });
      }
    });
  },

  remove: function(options) {
    var table = options.table;
    var key = options.key;
    return new Promise(function(resolve, reject) {
      if(tables[table]) {
        if(tables[table].key) {
          delete tables[table].key;
        }
      } else {
        reject({
          code : "400",
          error: "Table doesnt exist"
        });
      }
    });
  },

  dropTable: function(table) {
    return new Promise(function(resolve, reject) {
      if(tables[table]) {
        var data = {

        };
        data[table] = null;
        fbUtils.update({
          base: "fbFimConfig",
          path: "/",
          data: data
        }).then(function() {
          delete tableConfig[table];
          fbUtils.update({
            base: "fbFimStore",
            path: "/",
            data: data
          }).then(function(response) {
            delete tables[table];
            resolve(response);
          }, function(error) {
            //doesn't matter -- will get deleted on restart
            reject(error);
          });
        }, function(error) {
          reject(error);
        });
      } else {
        reject({
          code: "400",
          error: "Table not there"
        });
      }
    });
  }
}