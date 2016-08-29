'use strict'

var Promise = require('bluebird'),
  co = require('co'),
  helpers = require('./_helpers')

var _checkEdge = co.wrap(function * (edge) {
  var cleanedge = {docs: []}
  if (!edge || !helpers.isArray(edge.docs)) return Promise.reject('[docs] must be an array.')
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
var _inspectObject = co.wrap(function * (modules, api, param) {
  var rtn = {}, i1, required

  param = param || {}

  for (var key in api) {
    if (api.hasOwnProperty(key)) {
      if (helpers.isString(api[key])) {
        // String (we don't recurse from here)

        // Any
        i1 = helpers.getIndexOf(api[key], '@any')
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
        i1 = helpers.getIndexOf(api[key], '@string')
        if (i1 > -1) {
          required = (api[key].substr(i1 - 1, 2) == '@@')
          if (param[key]) {
            if (helpers.isString(param[key])) {
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
        i1 = helpers.getIndexOf(api[key], '@number')
        if (i1 > -1) {
          required = (api[key].substr(i1 - 1, 2) == '@@')
          if (param[key]) {
            if (helpers.isNumber(param[key])) {
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
        i1 = helpers.getIndexOf(api[key], '@date')
        if (i1 > -1) {
          required = (api[key].substr(i1 - 1, 2) == '@@')
          if (param[key]) {
            if (helpers.isDate(param[key])) {
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
        i1 = helpers.getIndexOf(api[key], '@bool')
        if (i1 > -1) {
          required = (api[key].substr(i1 - 1, 2) == '@@')
          if (helpers.isBool(param[key])) {
            rtn[key] = param[key]
            continue
          } else if (required) {
            throw new Error(key + ' is a required parameter for this API.')
          } else {
            continue
          }
        }

        // Array
        i1 = helpers.getIndexOf(api[key], '@array')
        if (i1 > -1) {
          required = (api[key].substr(i1 - 1, 2) == '@@')
          if (param[key]) {
            if (helpers.isArray(param[key])) {
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
        i1 = helpers.getIndexOf(api[key], '@object')
        if (i1 > -1) {
          required = (api[key].substr(i1 - 1, 2) == '@@')
          if (param[key]) {
            if (helpers.isObject(param[key])) {
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
      } else if (helpers.isObject(api[key])) {
        param[key] = param[key] || {}

        // Modules
        if (api[key]['@module']) {
          var modname = api[key]['@module']['name']
          var func = api[key]['@module']['function']
          var args = api[key]['@module']['args']
          var found = false

          for (var mod in modules) {
            if (modules[mod].type == 'value' && modules[mod].name == modname) {
              args.forEach(function (arg, idx) {
                if (helpers.isString(arg) && arg.substr(0, 1) == '#') {
                  args[idx] = param[arg.substr(1)]
                }
              })
              var apiFunc = eval("modules[mod]['module']." + func)
              rtn[key] = yield apiFunc.apply(null, args)
              found = true
            }
          }
          if (!found) {
            throw new Error('The API requires a module named [' + modname + '] to be defined.')
          }

          continue
        } else if (api[key]['@in'] || api[key]['@@in']) {
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
          rtn[key] = yield _inspectObject(modules, api[key], param[key])
        }

      /*
        APPLIES TO
        Query operators: $or, $nor, $and, $not. Requires array content.
      */
      } else if (helpers.isArray(api[key])) {
        rtn[key] = []
        for (var i = 0; i < api[key].length; i++) {
          for (var elementkey in api[key][i]) {
            var elem = yield _inspectObject(modules, api[key][i], param)
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
          o[i] = o[i].replace('@wm.now', new Date())

          for (var k = 0; k < api.globals.length; k++) {
            for (var prop in api.globals[k]) {
              o[i] = o[i].replace(prop, api.globals[k][prop])
            }
          }
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

// Public exports
module.exports = {
  replaceGlobals: _replaceGlobals,
  inspectInput: _inspectInput,
  inspectObject: _inspectObject,
  checkEdge: _checkEdge
}
