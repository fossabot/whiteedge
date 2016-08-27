
var uuid = require('node-uuid')

// Helpers
var _getUUID = function () {
  return uuid.v4().replace(/-/g, '')
}

var _getIndexOf = function (s, toSearch) {
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

// Public exports
module.exports = {
  isBool: _isBool,
  isArray: _isArray,
  isObject: _isObject,
  isString: _isString,
  isNumber: _isNumber,
  isDate: _isDate,
  getUUID: _getUUID,
  getIndexOf: _getIndexOf
}
