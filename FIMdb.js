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
    if(!tableConfig[name].isNotCached && tableConfig[name].isPersistent) {
      tables[name] = {};
      createReverseTables({
        id: tableConfig[name].id,
        fields: tableConfig[name].fields,
        name: name
      });
    }
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

function parseTable(options) {
  tables[options.name] = options.data;
  parseReverseTables(options);
}

function pushOnDataPresent(options) {
  var table = options.table;
  var data = options.data;
  var key = options.key;
  var value = options.value;
  var isNotCached = options.isNotCached;
  if(!isNotCached && tables[table]) {
    for(var k in value) {
      if(!tables[table][key]) {
        tables[table][key] = {};
      }
      if(!data) {
        data = {};
      }
      if(tableConfig[table].fields[k].isPrimary) {
        if(!tables[k][value[k]]) {
          tables[k][value[k]] = {};
        }
        tables[table][key][k] = value[k];
        data[k] = value[k];
        tables[k][value[k]][table] = key;
      } else if(tableConfig[table].fields[k].isOrdered) {
        if(!tables[table][key][k]) {
          tables[table][key][k] = [];
        } 
        if(!data[k]) {
          data[k] = [];
        }
        tables[table][key][k].push(value[k]);
        data[k].push(value[k]);
      } else {
        tables[table][key]=value;
        data = value;
      }
    }
  } else if(isNotCached) {
    for(var k in value) {
      if(!data) {
        data = {};
      }
      if(tableConfig[table].fields[k].isOrdered) {
        if(!data[k]) {
          data[k] = [];
        }
        data[k].push(value[k]);
      } else {
        data = value;
      }
    }
  }
  return data;
}

function fetchAndParseTable(name) {
  return new Promise(function(resolve, reject) {
    fbUtils.fetch({
      base: "fbFimStore",
      path: "/"+name
    }).then(function(response) {
      parseTable({
        name: name,
        data: response.data
      });
      resolve();
    }, function(error) {
      logger.error(error);
      resolve();
    });
  });
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
        var funcCalls = [];
        for(var name in tableConfig) {
          if(tableConfig[name] && !tableConfig[name].isNotCached && tableConfig[name].isPersistent) {
            funcCalls.push(fetchAndParseTable(name));
          }
        }
        Promise.all(funcCalls).then(function() {
          resolve();
        });
      }, function(error) {
        logger.error(error);
        resolve();
      });
    });
  },


  //DB APIs

  createTable: function(options) {
    var name = options.name;
    var fields = options.fields;
    var tableId = uuid.v4();
    var isPersistent = options.isPersistent || false;
    var isNotCached = options.isNotCached || false;
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
            isNotCached: isNotCached,
            fields: fields
          }
        }).then(function(response) {
          tableConfig[name] = {
            id: tableId,
            path: "/"+name,
            fields: fields,
            name: name,
            isPersistent: isPersistent,
            isNotCached: isNotCached
          };
          //local copy only if caching allowed
          if(!isNotCached) {
            tables[name] = {};
            createReverseTables({
              id: tableId,
              fields: fields,
              name: name
            });
          }
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
    logger.debug("FIMdb fetch: ", table, key);
    var curr, idx;
    if(tables[table]) {
      curr = tables[table];
      for(idx=0; idx<key.length; idx++) {
        if(curr[key[idx]]) {
          curr = curr[key[idx]];
        } else {
          curr = undefined;
          break;
        }
      }
      return curr;
    } else {
      if(tableConfig[table] && tableConfig[table].isNotCached && key.length > 0) {
        return new Promise(function(resolve, reject) {
          fbUtils.fetch({
            base: "fbFimStore",
            path: "/"+table+"/"+key[0]
          }).then(function(response) {
            var data = response.data;
            curr = response.data;
            for(idx=1; idx<key.length; idx++) {
              if(curr[key[idx]]) {
                curr = curr[key[idx]];
              } else {
                curr = undefined;
                break;
              }
            }
            resolve(curr);
          }, function(error) {
            resolve(null);
          });
        });
      } else {
        return false;
      }
    }
  },
  
  store: function(options) {
    var table = options.table;
    var key = options.key;
    var value = options.value;
    logger.debug("FIMdb store: ", table, key, value);
    var data, parsedData;
    return new Promise(function(resolve, reject) {
      if(tableConfig[table]) {
        if(tableConfig[table].isNotCached) {
          fbUtils.fetch({
            base: "fbFimStore",
            path: "/"+table+"/"+key
          }).then(function(response) {
            data = response.data;
            parsedData = pushOnDataPresent({
              data: data,
              table: table,
              key: key,
              value, value,
              isNotCached: true
            });
            fbUtils.update({
              base: "fbFimStore",
              path: "/"+table+"/"+key,
              data: parsedData
            }).then(function(response) {
              resolve(response);
            }, function(error) {
              //remove the local data
              if(data && !tableConfig[table].isNotCached) {
                tables[table][key] = data;
              } else {
                delete tables[table][key];
              }
              reject(error);
            });
          }, function(error) {
            parsedData = pushOnDataPresent({
              data: null,
              table: table,
              key: key,
              value, value,
              isNotCached: true
            });
            fbUtils.update({
              base: "fbFimStore",
              path: "/"+table+"/"+key,
              data: parsedData
            }).then(function(response) {
              resolve(response);
            }, function(error) {
              reject(error);
            });
          });
        } else {
          data = tables[table] && tables[table][key] ? tables[table][key]: null;
          parsedData = pushOnDataPresent({
            data: data,
            table: table,
            key: key,
            value: value,
            isNotCached: false
          });
          fbUtils.update({
            base: "fbFimStore",
            path: "/"+table+"/"+key,
            data: parsedData
          }).then(function(response) {
            resolve(response);
          }, function(error) {
            //remove the local data
            if(oldValue && !tableConfig[table].isNotCached) {
              tables[table][key] = data;
            } else {
              delete tables[table][key];
            }
            reject(error);
          });
        }
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
    logger.debug("FIMdb remove: ", table, key);
    return new Promise(function(resolve, reject) {
      if(tableConfig[table]) {
        if(tables[table] && tables[table][key]) {
          for(var field in tableConfig[table].fields) {
            if(tableConfig[table].fields[field].isPrimary) {
              if(tables[field] && tables[field][tables[table][key][field]] && tables[field][tables[table][key][field]][table]) {
                delete tables[field][tables[table][key][field]][table];
                logger.debug("FIMdb remove: deleted intermediate ", field, tables[table][key][field], table);
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
          logger.debug("FIMdb remove: deleted final ", table, key);
        }
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
