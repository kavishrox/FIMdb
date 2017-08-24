var uuid = require('node-uuid');
var Promise = require('bluebird');
//fast in memory database
var logger, fbUtils;

var tableConfig = {};
var tables = {};

function parseConfig(options) {
  fbUtils = options.fbUtils;
  logger = options.logger;
}

function parseTableConfig(data) {
  for(var name in data) {
    tableConfig[name] = data[name];
    tables[name] = {};
    createReverseTables({
      id: tableConfig[name].id,
      fields: tableConfig[name].fields,
      name: name
    });
  }
}

function createReverseTables(options) {
  var id = options.id;
  var fields = options.fields;
  var name = options.name;
  return new Promise(function(resolve, reject) {
    for(var field in fields) {
      if(fields[field].isPrimary) {
        //create reverse entry
        var newFields = {

        };
        newFields[name] = {
          isPrimary: true
        }
        tableConfig[field] = {
          id: uuid.v4(),
          referenceId: id,
          referenceName: name,
          fields: newFields,
          name: field,
          path: null
        };
        tables[field] = {};
      }
    }
  });
}

function parseReverseTables(options) {
  var data = options.data;
  var name = options.name;
  for(var key in data) {
    for(var subkey in data[key]) {
      if(tableConfig[name].fields[subkey].isPrimary) {
        if(!tables[subkey][data[key][subkey]]) {
          tables[subkey][data[key][subkey]] = {};
        }
        tables[subkey][data[key][subkey]][name] = key;
      }
    }
  }
}

function parseTables(data) {
  for(var name in data) {
    if(tableConfig[name] && tableConfig[name].isPersistent) {
      tables[name] = data[name];
      parseReverseTables({
        name: name,
        data: tables[name]
      });
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
    return new Promise(function(resolve) {
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
          resolve();
        }, function(error) {
          logger.error(error);
          resolve();
        });
      }, function(error) {
        logger.error(error);
        resolve();
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
          base: "fbFimConfig",
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
            fields: fields,
            name: name,
            isPersistent: isPersistent
          };
          tables[name] = {};
          createReverseTables({
            id: tableId,
            fields: fields,
            name: name
          });
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
      return curr;
    } else {
      return false;
    }
  },
  
  store: function(options) {
    var table = options.table;
    var key = options.key;
    var value = options.value;
    return new Promise(function(resolve, reject) {
      if(tables[table]) {
        var oldValue = tables[table][key];
        tables[table][key] = value;
        var data = {

        };
        data[key] = value;
        for(var k in value) {
          if(tableConfig[table].fields[k].isPrimary) {
            if(!tables[k][value[k]]) {
              tables[k][value[k]] = {};
            }
            tables[k][value[k]][table] = key;
          }
        }
        fbUtils.update({
          base: "fbFimStore",
          path: "/"+table,
          data: data
        }).then(function(response) {
          resolve(response);
        }, function(error) {
          //remove the local data
          if(oldValue) {
            tables[table][key] = oldValue;
          } else {
            delete tables[table][key];
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
        if(tables[table][key]) {
          for(var field in tableConfig[table].fields) {
            if(tableConfig[table].fields[field].isPrimary) {
              if(tables[field] && tables[field][tables[table][key][field]] && tables[field][tables[table][key][field]][table]) {
                delete tables[field][tables[table][key][field]][table];
                fbUtils.set({
                  base: "fbFimStore",
                  path: "/"+field+"/"+tables[table][key][field],
                  data: null
                }).then(function() {
                  // resolve(response);
                }, function() {
                  //removed from the local anyways
                  // reject(error);
                });
                if(Object.keys(tables[field][tables[table][key][field]]).length === 0) {
                  delete tables[field][tables[table][key][field]];
                }
              }
            }
          }
          delete tables[table][key];
          fbUtils.set({
            base: "fbFimStore",
            path: "/"+table+"/"+key,
            data: null
          }).then(function(response) {
            resolve(response);
          }, function(error) {
            //removed from the local anyways
            reject(error);
          });
        } else {
          reject({
            code: "400",
            error: "No entry available for this key"
          });
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