'use strict'

try {
  var db = require(`${__state.dirs.server}/lib/db`),
    Promise = require('bluebird'),
    co = require('co'),
    uuid = require('node-uuid')
} catch (err) {
  console.log(err)
}

// Helpers
var _getUUID = function () {
  return uuid.v4().replace(/-/g, '')
}

var getIndexOf = function (s, toSearch) {
  return s.indexOf(toSearch)
}

var _isArray = function (a) {
  return (!!a) && (Array.isArray(a))
}

var _isObject = function (a) {
  return (!!a) && (a.constructor === Object)
}

var _isString = function (a) {
  return (typeof a === 'string')
}

var _isBool = function (a) {
  return (typeof a === 'boolean')
}

var _isNumber = function (a) {
  return (typeof a === 'number')
}

var _isDate = function (a) {
  return (Object.prototype.toString.call(a) === '[object Date]')
}

var _checkEdge = co.wrap(function * (edge) {
  var cleanedge = {docs: []}
  if (!edge || !_isArray(edge.docs)) return Promise.reject('[docs] must be an array.')
  if (!edge.docs.length == 2) return Promise.reject('The length of the [docs] array must be 2.')
  if (!edge.predicate) return Promise.reject('[predicate] is missing.')
  if (edge.data) cleanedge.data = edge.data
  cleanedge.predicate = edge.predicate
  edge.docs.forEach(function (edge) {
    if (!edge.id) return Promise.reject('The edge is missing a [id] field.')
    if (!edge.col) return Promise.reject('The edge is missing a [col] field.')
    var newedge = {
      id: edge.id,
      col: edge.col
    }
    if (edge.vertice) newedge.vertice = edge.vertice
    cleanedge.docs.push(newedge)
  })

  return Promise.resolve(cleanedge)
})

// Inspect API parameters
var _inspectObject = co.wrap(function * (api, param) {
  var rtn = {}, i1, required

  param = param || {}

  for (var key in api) {
    if (api.hasOwnProperty(key)) {
      if (_isString(api[key])) {
        // String (we don't recurse from here)

        // Any
        i1 = getIndexOf(api[key], '@any')
        if (i1 > -1) {
          required = (api[key].substr(i1 - 1, 2) == '@@')
          if (param[key]) {
            rtn[key] = param[key]
            continue
          } else if (required) {
            throw new Error(key + ' is a required parameter for this API.')
          } else {
            continue
          }
        }

        // String
        i1 = getIndexOf(api[key], '@string')
        if (i1 > -1) {
          required = (api[key].substr(i1 - 1, 2) == '@@')
          if (param[key]) {
            if (_isString(param[key])) {
              rtn[key] = param[key]
              continue
            } else {
              throw new Error(key + ' must be a string.')
            }
          } else if (required) {
            throw new Error(key + ' is a required parameter for this API.')
          } else {
            continue
          }
        }

        // Number
        i1 = getIndexOf(api[key], '@number')
        if (i1 > -1) {
          required = (api[key].substr(i1 - 1, 2) == '@@')
          if (param[key]) {
            if (_isNumber(param[key])) {
              rtn[key] = param[key]
              continue
            } else {
              throw new Error(key + ' must be a number.')
            }
          } else if (required) {
            throw new Error(key + ' is a required parameter for this API.')
          } else {
            continue
          }
        }

        // Date
        i1 = getIndexOf(api[key], '@date')
        if (i1 > -1) {
          required = (api[key].substr(i1 - 1, 2) == '@@')
          if (param[key]) {
            if (_isDate(param[key])) {
              rtn[key] = param[key]
              continue
            } else {
              throw new Error(key + ' must be a date.')
            }
          } else if (required) {
            throw new Error(key + ' is a required parameter for this API.')
          } else {
            continue
          }
        }

        // Boolean
        i1 = getIndexOf(api[key], '@bool')
        if (i1 > -1) {
          required = (api[key].substr(i1 - 1, 2) == '@@')
          if (_isBool(param[key])) {
            rtn[key] = param[key]
            continue
          } else if (required) {
            throw new Error(key + ' is a required parameter for this API.')
          } else {
            continue
          }
        }

        // Array
        i1 = getIndexOf(api[key], '@array')
        if (i1 > -1) {
          required = (api[key].substr(i1 - 1, 2) == '@@')
          if (param[key]) {
            if (_isArray(param[key])) {
              rtn[key] = param[key]
              continue
            } else {
              throw new Error(key + ' must be an array.')
            }
          } else if (required) {
            throw new Error(key + ' is a required parameter for this API.')
          } else {
            continue
          }
        }

        // Object
        i1 = getIndexOf(api[key], '@object')
        if (i1 > -1) {
          required = (api[key].substr(i1 - 1, 2) == '@@')
          if (param[key]) {
            if (_isObject(param[key])) {
              rtn[key] = param[key]
              continue
            } else {
              throw new Error(key + ' must be an object.')
            }
          } else if (required) {
            throw new Error(key + ' is a required parameter for this API.')
          } else {
            continue
          }
        }

        // If we reach this point, the API overwrites whatever the parameter is.        
        if (i1 == -1) {
          rtn[key] = api[key]
        }

      /*
        APPLIES TO
        Update operators: $set, $inc, $mul
        Sub-operations like $exists, $push.
      */
      } else if (_isObject(api[key])) {
        param[key] = param[key] || {}

        // Modules
        if (api[key]['@in'] || api[key]['@@in']) {
          // In array
          var arr
          if (api[key]['@@in']) {
            arr = api[key]['@@in']
            required = true
          } else {
            arr = api[key]['@in']
            required = false
          }
          arr.forEach(function (element) {
            if (element == param[key]) {
              rtn[key] = param[key]
            }
          })
          if (required && !rtn[key]) {
            throw new Error(key + ' is a required parameter for this API. Allowed values are [' + arr.toString() + '].')
          } else {
            continue
          }
        } else if (api[key]['@range'] || api[key]['@@range']) {
          // Ranges
          var arr
          if (api[key]['@@range']) {
            arr = api[key]['@@range']
            required = true
          } else {
            arr = api[key]['@range']
            required = false
          }

          if (required && !rtn[key]) {
            throw new Error(key + ' is a required parameter for this API. Allowed values are [' + arr.toString() + '].')
          } else {
            if (param[key] >= arr[0] && param[key] <= arr[1]) {
              rtn[key] = param[key]
              continue
            } else {
              throw new Error(key + ' is not within the allowed range [' + arr.toString() + '].')
            }
          }
        } else {
          rtn[key] = yield _inspectObject(api[key], param[key])
        }

      /*
        APPLIES TO
        Query operators: $or, $nor, $and, $not. Requires array content.
      */
      } else if (_isArray(api[key])) {
        rtn[key] = []
        for (var i = 0; i < api[key].length; i++) {
          for (var elementkey in api[key][i]) {
            var elem = yield _inspectObject(api[key][i], param)
            rtn[key].push(elem)
          }
        }
      } else {
        // Date, number, or bool
        rtn[key] = api[key]
      }
    }
  }

  return Promise.resolve(rtn)
})

// TODO: Put users in redis?
var _replaceGlobals = co.wrap(function * (api) {
  function traverse (o) {
    for (var i in o) {
      if (o[i] !== null) {
        if (typeof o[i] == 'object') {
          traverse(o[i]); // going on step down in the object tree!!
        } else if (typeof o[i] == 'string' && o[i].length > 0) {
          o[i] = o[i].replace(/@wm.now/g, new Date())
        }
      }
    }
  }

  traverse(api)
  return Promise.resolve(api)
})

// Inspect an input type of field (limits and skips)
var _inspectInput = co.wrap(function * (name, apival, paramval, emptyAllowed) {
  var rtn = null
  // Limit
  if (apival) {
    var i = apival.indexOf('-')
    if (i > -1) {
      if (paramval) {
        var minValue = parseFloat(apival.substr(0, i))
        var maxValue = parseFloat(apival.substring(i + 1))
        var paramLimit = parseFloat(paramval)

        if (paramLimit > maxValue || paramLimit < minValue) {
          throw new Error(name + ' must be between ' + minValue + ' and ' + maxValue + '.')
        } else {
          rtn = parseFloat(paramLimit)
        }
      } else if (!emptyAllowed) {
        throw new Error(name + ' must be between ' + apival + '.')
      }
    } else {
      rtn = parseFloat(apival)
    }
  }
  return Promise.resolve(rtn)
})

// Get the API for a Mongo call
var _getMongoAPI = co.wrap(function * (params) {
  var apidoc = yield db.col(params.hostname, '_apis').find({'code': params.use}).limit(1).next()

  if (apidoc) {
    if (apidoc.method == params.mongocall) {
      params.apidoc = apidoc
      return Promise.resolve(params)
    } else {
      return Promise.reject('You called a ' + params.mongocall + ' api: ' + params.use + ', but this API is for ' + apidoc.method + ' calls.')
    }
  } else {
    return Promise.reject('API not found: ' + params.use)
  }
})

// Get the API for a Mongo call
var _getEdgeAPI = co.wrap(function * (params) {
  var apidoc = yield db.col(params.hostname, '_apis').find({'code': params.use}).limit(1).next()

  if (apidoc) {
    if (apidoc.method == params.edgecall) {
      params.apidoc = apidoc
      return Promise.resolve(params)
    } else {
      return Promise.reject('You called a ' + params.edgecall + ' api: ' + params.use + ', but this API is for ' + apidoc.method + ' calls.')
    }
  } else {
    return Promise.reject('API not found: ' + params.use)
  }
})

// Public exports
module.exports = {
  replaceGlobals: _replaceGlobals,
  inspectInput: _inspectInput,
  inspectObject: _inspectObject,
  getMongoAPI: _getMongoAPI,
  getEdgeAPI: _getEdgeAPI,
  checkEdge: _checkEdge,
  isBool: _isBool,
  isArray: _isArray,
  isObject: _isObject,
  isString: _isString,
  isNumber: _isNumber,
  isDate: _isDate,
  getUUID: _getUUID
}
