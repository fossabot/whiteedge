'use strict'

// Modules
var Promise = require('bluebird'),
  co = require('co'),
  lib = require('./lib/_lib'),
  helpers = require('./lib/_helpers'),
  requireFromString = require('require-from-string')

// Client
function EdgeClient (db, api) {
  var vars = {
    api: api, // api document or string
    query: null, // MongoDB parameter
    doc: null, // MongoDB parameter
    docs: [], // MongoDB parameter
    filter: null, // MongoDB parameter
    update: null, // MongoDB parameter
    replacement: null, // MongoDB parameter
    pipeline: null, // MongoDB options
    sort: null, // MongoDB options
    project: null, // MongoDB options
    limit: null, // MongoDB options
    skip: null, // MongoDB options
    edges: null, // MongoDB options
    options: null, // MongoDB options,
    _apis: {}, // Cached api's
    globals: [], // User defined global variables
    collection: null // MongoDB collection
  }

  vars.api = api

  /**
   * Public instance methods.
   * These methods have access to private instance variables and methods.
   */

  this.globals = function (docs) {
    vars.globals = docs
    return this
  }

  // find  
  this.find = function (query, options) {
    vars.query = query
    vars.options = options
    vars.type = 'mongo'
    vars.method = 'find'
    return this
  }

  // upsertEdge  
  this.upsertEdge = co.wrap(function * (edge, update) {
    var results
    yield _getAPI()
    yield _executePreModules()

    vars.api = yield lib.replaceGlobals(vars.api)
    edge.docs[0] = yield lib.inspectObject(vars.api.modules, vars.api.edge.docs[0], edge.docs[0])
    edge.docs[1] = yield lib.inspectObject(vars.api.modules, vars.api.edge.docs[1], edge.docs[1])
    edge.predicate = vars.api.edge.predicate
    edge.data = yield lib.inspectObject(vars.api.modules, vars.api.data, edge.data)

    vars.edge = yield lib.checkEdge(edge)
    vars.update = yield lib.inspectObject(vars.api.modules, vars.api.update, update)

    var query = {
      predicate: vars.edge.predicate,
      edges: { $all: vars.edge.docs }
    }
    // Clean up the query object, in case client has added extra attributes to the edge object.
    var results = yield db.collection('_edges').find(query).limit(1).next()

    if (!results) {
      results = yield this.edgeInsertOne(vars.edge)
    }

    // Move the scope of the fields in the update to 'data'
    for (var op in vars.update) {
      for (var field in vars.update[op]) {
        vars.update[op]['data.' + field] = vars.update[op][field]
        delete (vars.update[op][field])
      }
    }
    yield db.collection('_edges').findOneAndUpdate({
      _id: results._id
    }, vars.update)

    yield _executePostModules()
    return Promise.resolve(results)
  })

  // deleteEdge  
  this.deleteEdge = co.wrap(function * (edge) {
    yield _getAPI()
    yield _executePreModules()

    vars.api = yield lib.replaceGlobals(vars.api)
    edge.predicate = vars.api.edge.predicate
    edge.docs[0] = yield lib.inspectObject(vars.api.modules, vars.api.edge.docs[0], edge.docs[0])
    edge.docs[1] = yield lib.inspectObject(vars.api.modules, vars.api.edge.docs[1], edge.docs[1])

    vars.edge = yield lib.checkEdge(edge)

    var query = {
      predicate: vars.edge.predicate,
      edges: { $all: vars.edge.docs }
    }

    var results = yield db.collection('_edges').findOneAndDelete(query)
    yield _executePostModules()
    return Promise.resolve(results.value._id)
  })

  // insertEdge  
  this.insertEdge = co.wrap(function * (edge) {
    yield _getAPI()
    yield _executePreModules()
    
    vars.api = yield lib.replaceGlobals(vars.api)
    edge.docs[0] = yield lib.inspectObject(vars.api.modules, vars.api.edge.docs[0], edge.docs[0])
    edge.docs[1] = yield lib.inspectObject(vars.api.modules, vars.api.edge.docs[1], edge.docs[1])
    edge.predicate = vars.api.edge.predicate
    edge.data = yield lib.inspectObject(vars.api.modules, vars.api.data, edge.data)

    vars.edge = yield lib.checkEdge(edge)

    var query = {
      predicate: vars.edge.predicate,
      edges: { $all: vars.edge.docs }
    }
    var results = yield db.collection('_edges').find(query).limit(1).next()

    if (results) {
      yield _executePostModules()
      return Promise.resolve(results)
    } else {
      var cleanedge = {
        _id: helpers.getUUID(),
        predicate: vars.edge.predicate,
        edges: vars.edge.docs
      }
      if (vars.edge.data) cleanedge.data = vars.edge.data

      // Get source and target documents
      cleanedge.docs = []
      cleanedge.docs[0] = yield db.collection(cleanedge.edges[0].col).find({_id: cleanedge.edges[0].id}).limit(1).next()
      cleanedge.docs[1] = yield db.collection(cleanedge.edges[1].col).find({_id: cleanedge.edges[1].id}).limit(1).next()

      if (!cleanedge.docs[0]) return Promise.reject('A document specified in [edge] was not found. _id: ' + cleanedge.edges[0].id)
      if (!cleanedge.docs[1]) return Promise.reject('A document specified in [edge] was not found. _id: ' + cleanedge.edges[1].id)

      yield db.collection('_edges').findOneAndUpdate(query, cleanedge, { upsert: true })
      yield _executePostModules()
      return Promise.resolve(cleanedge)
    }
  })

  // aggregate  
  this.aggregate = co.wrap(function * (pipeline, options) {
    yield _getAPI()
    yield _executePreModules()

    if (!vars.api.collection) throw new Error('Collection parameter is missing.')
    vars.api = yield lib.replaceGlobals(vars.api)
    vars.options = yield lib.inspectObject(vars.api.modules, vars.api.options, options)
    if (!vars.api.pipeline) throw new Error('API: aggregate requires a [pipeline] element, which was not supplied in the API.')
    if (!helpers.isArray(vars.api.pipeline)) throw new Error('API: aggregate requires a [pipeline] element (array), which was not supplied in the API.')

    if (pipeline) {
      for (var i = 0; i < vars.api.pipeline.length; i++) {
        var apikey = null
        // Get the API key          
        for (var key in vars.api.pipeline[i]) {
          apikey = key
          break
        }

        // Get the value key          
        for (var i2 = 0; i2 < vars.api.pipeline.length; i2++) {
          for (var key in pipeline[i2]) {
            if (key == apikey) {
              vars.pipeline[i2] = yield lib.inspectObject(vars.api.modules, vars.api.pipeline[i], pipeline[i2])
              break
            }
          }
        }
      }
    } else {
      vars.pipeline = vars.api.pipeline
    }
    var results = yield db.collection(vars.api.collection).aggregate(vars.pipeline, vars.options).toArray()
    yield _executePostModules()
    return Promise.resolve(results)
  })
  // insertOne  
  this.insertOne = co.wrap(function * (doc, options) {
    yield _getAPI()
    yield _executePreModules()

    if (!vars.api.collection) throw new Error('Collection parameter is missing.')
    vars.api = yield lib.replaceGlobals(vars.api)
    vars.options = yield lib.inspectObject(vars.api.modules, vars.api.options, options)
    if (!vars.api.doc) throw new Error('API: insertOne requires a [doc] element, which was not supplied in the API.')
    vars.doc = yield lib.inspectObject(vars.api.modules, vars.api.doc, doc)
    vars.doc._id = vars.doc._id || helpers.getUUID()
    yield db.collection(vars.api.collection).insertOne(vars.doc, vars.options)
    yield _executePostModules()
    return Promise.resolve(vars.doc)
  })
  // insertMany  
  this.insertMany = co.wrap(function * (docs, options) {
    yield _getAPI()
    yield _executePreModules()

    if (!vars.api.collection) throw new Error('Collection parameter is missing.')
    vars.api = yield lib.replaceGlobals(vars.api)
    vars.options = yield lib.inspectObject(vars.api.modules, vars.api.options, options)
    if (!vars.api.doc) throw new Error('API: insertMany requires a [doc] element, which was not supplied in the API.')
    for (var i = 0; i < docs.length; i++) {
      vars.docs[i] = yield lib.inspectObject(vars.api.modules, vars.api.doc, docs[i])
      vars.docs[i]._id = docs[i]._id || helpers.getUUID()
    }
    var results = yield db.collection(vars.api.collection).insertMany(vars.docs, vars.options)
    yield _executePostModules()
    return Promise.resolve(results)
  })

  // replaceOne  
  this.replaceOne = co.wrap(function * (filter, doc, options) {
    yield _getAPI()
    yield _executePreModules()

    if (!vars.api.collection) throw new Error('Collection parameter is missing.')
    vars.api = yield lib.replaceGlobals(vars.api)
    vars.options = yield lib.inspectObject(vars.api.modules, vars.api.options, options)
    if (!vars.api.filter) throw new Error('API: replaceOne requires a [filter] element, which was not supplied in the API.')
    if (!vars.api.doc) throw new Error('API: replaceOne requires a [doc] element, which was not supplied in the API.')
    vars.filter = yield lib.inspectObject(vars.api.modules, vars.api.filter, filter)
    vars.doc = yield lib.inspectObject(vars.api.modules, vars.api.doc, doc)
    var results = yield db.collection(vars.api.collection).replaceOne(vars.filter, vars.doc, vars.options)

    // Update edges        
    if (results.result.nModified > 0) {
      var edgeFilter, setOp
      for (var i = 0; i < 2; i++) {
        edgeFilter = {}, setOp = { $set: {} }
        for (var op in vars.filter) {
          edgeFilter['docs.' + i + '.' + op] = vars.filter[op]
        }
        setOp['$set']['docs.' + i] = doc
        yield db.collection('_edges').updateMany(edgeFilter, setOp)
      }
    }

    yield _executePostModules()
    return Promise.resolve(results)
  })

  // updateMany  
  this.updateMany = co.wrap(function * (filter, update, options) {
    yield _getAPI()
    yield _executePreModules()

    if (!vars.api.collection) throw new Error('Collection parameter is missing.')
    vars.api = yield lib.replaceGlobals(vars.api)
    vars.options = yield lib.inspectObject(vars.api.modules, vars.api.options, options)
    if (!vars.api.filter) throw new Error('API: updateMany requires a [filter] element, which was not supplied in the API.')
    if (!vars.api.update) throw new Error('API: updateMany requires a [update] element, which was not supplied in the API.')
    vars.filter = yield lib.inspectObject(vars.api.modules, vars.api.filter, filter)
    vars.update = yield lib.inspectObject(vars.api.modules, vars.api.update, update)

    // Update all the documents        
    var results = yield db.collection(vars.api.collection).updateMany(vars.filter, vars.update, vars.options)

    // Update edges        
    if (results.result.nModified > 0) {
      var edgeFilter, edgeUpdate
      for (var i = 0; i < 2; i++) {
        edgeFilter = {}, edgeUpdate = {}
        for (var op in vars.filter) {
          edgeFilter['docs.' + i + '.' + op] = vars.filter[op]
        }
        for (var op in vars.update) {
          edgeUpdate[op] = {}
          for (var field in vars.update[op]) {
            edgeUpdate[op]['docs.' + i + '.' + field] = vars.update[op][field]
          }
        }
        yield db.collection('_edges').updateMany(edgeFilter, edgeUpdate)
      }
    }

    yield _executePostModules()
    return Promise.resolve(results)
  })

  // updateOne  
  this.updateOne = co.wrap(function * (filter, update, options) {
    yield _getAPI()
    yield _executePreModules()

    if (!vars.api.collection) throw new Error('Collection parameter is missing.')
    vars.api = yield lib.replaceGlobals(vars.api)
    vars.options = yield lib.inspectObject(vars.api.modules, vars.api.options, options)
    if (!vars.api.filter) throw new Error('API: updateOne requires a [filter] element, which was not supplied in the API.')
    if (!vars.api.update) throw new Error('API: updateOne requires a [update] element, which was not supplied in the API.')
    vars.filter = yield lib.inspectObject(vars.api.modules, vars.api.filter, filter)
    vars.update = yield lib.inspectObject(vars.api.modules, vars.api.update, update)

    // Update the document        
    var results = yield db.collection(vars.api.collection).updateOne(vars.filter, vars.update, vars.options)

    // Update edges        
    if (results.result.nModified > 0) {
      var edgeFilter, edgeUpdate
      for (var i = 0; i < 2; i++) {
        edgeFilter = {}, edgeUpdate = {}
        for (var op in vars.filter) {
          edgeFilter['docs.' + i + '.' + op] = vars.filter[op]
        }
        for (var op in vars.update) {
          edgeUpdate[op] = {}
          for (var field in vars.update[op]) {
            edgeUpdate[op]['docs.' + i + '.' + field] = vars.update[op][field]
          }
        }
        yield db.collection('_edges').updateMany(edgeFilter, edgeUpdate)
      }
    }

    yield _executePostModules()
    return Promise.resolve(results)
  })

  // findOneAndUpdate  
  this.findOneAndUpdate = co.wrap(function * (filter, update, options) {
    yield _getAPI()
    yield _executePreModules()

    if (!vars.api.collection) throw new Error('Collection parameter is missing.')
    vars.api = yield lib.replaceGlobals(vars.api)
    vars.options = yield lib.inspectObject(vars.api.modules, vars.api.options, options)
    if (!vars.api.filter) throw new Error('API: findOneAndUpdate requires a [filter] element, which was not supplied in the API.')
    if (!vars.api.update) throw new Error('API: findOneAndUpdate requires a [update] element, which was not supplied in the API.')
    vars.filter = yield lib.inspectObject(vars.api.modules, vars.api.filter, filter)
    vars.update = yield lib.inspectObject(vars.api.modules, vars.api.update, update)

    // Update the document        
    var results = yield db.collection(vars.api.collection).findOneAndUpdate(vars.filter, vars.update, vars.options)

    // Update edges
    if (results.ok == 1) {
      var edgeFilter, edgeUpdate
      for (var i = 0; i < 2; i++) {
        edgeFilter = {}, edgeUpdate = {}
        for (var op in vars.filter) {
          edgeFilter['docs.' + i + '.' + op] = vars.filter[op]
        }
        for (var op in vars.update) {
          edgeUpdate[op] = {}
          for (var field in vars.update[op]) {
            edgeUpdate[op]['docs.' + i + '.' + field] = vars.update[op][field]
          }
        }
        yield db.collection('_edges').updateMany(edgeFilter, edgeUpdate)
      }
    }

    yield _executePostModules()
    return Promise.resolve(results)
  })

  // findOneAndDelete  
  this.findOneAndDelete = co.wrap(function * (filter, options) {
    yield _getAPI()
    yield _executePreModules()

    if (!vars.api.collection) throw new Error('Collection parameter is missing.')
    vars.api = yield lib.replaceGlobals(vars.api)
    vars.options = yield lib.inspectObject(vars.api.modules, vars.api.options, options)
    if (!vars.api.filter) throw new Error('API: findOneAndDelete requires a [filter] element, which was not supplied in the API.')
    vars.filter = yield lib.inspectObject(vars.api.modules, vars.api.filter, filter)

    // Update the document        
    var results = yield db.collection(vars.api.collection).findOneAndDelete(vars.filter, vars.options)

    // Update edges
    if (results.lastErrorObject.n == 1) {
      yield db.collection('_edges').deleteMany({ 'edges.id': results.value._id })
    }

    yield _executePostModules()
    return Promise.resolve(results)
  })

  // deleteOne  
  this.deleteOne = co.wrap(function * (filter, options) {
    yield _getAPI()
    yield _executePreModules()

    if (!vars.api.collection) throw new Error('Collection parameter is missing.')
    vars.api = yield lib.replaceGlobals(vars.api)
    vars.options = yield lib.inspectObject(vars.api.modules, vars.api.options, options)
    if (!vars.api.filter) throw new Error('API: deleteOne requires a [filter] element, which was not supplied in the API.')
    vars.filter = yield lib.inspectObject(vars.api.modules, vars.api.filter, filter)
    var results = yield db.collection(vars.api.collection).deleteOne(vars.filter, vars.options)

    // Update edges
    if (results.deletedCount == 1) {
      var edgeFilter
      for (var i = 0; i < 2; i++) {
        edgeFilter = {}
        for (var op in vars.filter) {
          edgeFilter['docs.' + i + '.' + op] = vars.filter[op]
        }
        yield db.collection('_edges').deleteMany(edgeFilter)
      }
    }

    yield _executePostModules()
    return Promise.resolve(results)
  })

  // deleteMany  
  this.deleteMany = co.wrap(function * (filter, options) {
    yield _getAPI()
    yield _executePreModules()

    if (!vars.api.collection) throw new Error('Collection parameter is missing.')
    vars.api = yield lib.replaceGlobals(vars.api)
    vars.options = yield lib.inspectObject(vars.api.modules, vars.api.options, options)
    if (!vars.api.filter) throw new Error('API: deleteMany requires a [filter] element, which was not supplied in the API.')
    vars.filter = yield lib.inspectObject(vars.api.modules, vars.api.filter, filter)
    var results = yield db.collection(vars.api.collection).deleteMany(vars.filter, vars.options)

    // Update edges        
    if (results.result.nModified > 0) {
      var edgeFilter
      for (var i = 0; i < 2; i++) {
        edgeFilter = {}
        for (var op in vars.filter) {
          edgeFilter['docs.' + i + '.' + op] = vars.filter[op]
        }
        yield db.collection('_edges').deleteMany(edgeFilter)
      }
    }

    yield _executePostModules()
    return Promise.resolve(results)
  })

  // findOneAndReplace  
  this.findOneAndReplace = co.wrap(function * (filter, replacement, options) {
    yield _getAPI()
    yield _executePreModules()

    if (!vars.api.collection) throw new Error('Collection parameter is missing.')
    vars.api = yield lib.replaceGlobals(vars.api)
    vars.options = yield lib.inspectObject(vars.api.modules, vars.api.options, options)
    if (!vars.api.filter) throw new Error('API: findOneAndReplace requires a [filter] element, which was not supplied in the API.')
    if (!vars.api.replacement) throw new Error('API: findOneAndReplace requires a [replacement] element, which was not supplied in the API.')
    vars.filter = yield lib.inspectObject(vars.api.modules, vars.api.filter, filter)
    vars.replacement = yield lib.inspectObject(vars.api.modules, vars.api.replacement, replacement)

    // Update the document        
    var results = yield db.collection(vars.api.collection).findOneAndReplace(vars.filter, vars.replacement, vars.options)

    // Update edges        
    if (results.ok == 1) {
      yield db.collection('_edges').updateMany({ 'edges.0.id': results.value._id }, { $set: { 'docs.0': vars.replacement }})
      yield db.collection('_edges').updateMany({ 'edges.1.id': results.value._id }, { $set: { 'docs.1': vars.replacement }})
    }

    yield _executePostModules()
    return Promise.resolve(results)
  })

  // Cursor values  
  this.sort = function (sort) {
    vars.sort = sort
  }
  this.project = function (project) {
    vars.project = project
  }
  this.limit = function (limit) {
    vars.limit = limit
  }
  this.skip = function (skip) {
    vars.skip = skip
  }

  // Currently only for find(), but in future for aggregate()  
  this.toArray = co.wrap(function * () {
    yield _getAPI()
    if (!vars.api.collection) throw new Error('Collection parameter is missing.')
    vars.api = yield lib.replaceGlobals(vars.api)
    vars.options = yield lib.inspectObject(vars.api.modules, vars.api.options, vars.options)

    switch (vars.method) {
      case 'find':
        yield _executePreModules()

        vars.query = yield lib.inspectObject(vars.api.modules, vars.api.query, vars.query)
        vars.sort = yield lib.inspectObject(vars.api.modules, vars.api.sort, vars.sort)
        vars.project = yield lib.inspectObject(vars.api.modules, vars.api.project, vars.project)
        vars.limit = yield lib.inspectInput('Limit', vars.api.limit, vars.limit, false)
        vars.skip = yield lib.inspectInput('Skip', vars.api.skip, vars.skip, true)

        var cursor = db.collection(vars.api.collection).find(vars.query)
        if (vars.sort) cursor = cursor.sort(vars.sort)
        if (vars.project) cursor = cursor.project(vars.project)
        if (vars.skip) cursor = cursor.skip(vars.skip)
        if (vars.limit) cursor = cursor.limit(vars.limit)

        // We give the client the cursor count (happy client!)
        var totalcount = yield cursor.count(false)
        var limitcount = yield cursor.count(false)
        var results = yield cursor.toArray()

        // Return results and total count
        if (results.length > 0) {
          // Traverse edges
          let newresults = yield Promise.all(
            results.map(result => _findNodes(result, vars.api.edges).then(parent => {
              // Add any data to the parent
              return parent
            }))
          )

          results = {
            documents: newresults,
            limitcount: limitcount,
            totalcount: totalcount
          }
        } else {
          results = {
            documents: [],
            cursorcount: 0
          }
        }
        yield _executePostModules()
        return Promise.resolve(results)

      default:
        throw new Error('Method not supported: ' + vars.method)
    }
  })

  /**
   * Private instance methods.
   * These methods have access to privat instance variables and methods.
   */

  // Load post modules into memory state, and execute them.
  var _executePostModules = co.wrap(function* () {
    if (vars.api.modules) {
      for (var mod in vars.api.modules) {
        if (vars.api.modules[mod].type == 'post') {
          yield vars.api.modules[mod]['module'](vars)
        }
      }
    }
    return Promise.resolve()
  })

  // Load post modules into memory state, and execute them.
  var _executePreModules = co.wrap(function * () {
    if (vars.api.modules) {
      for (var mod in vars.api.modules) {
        if (vars.api.modules[mod].type == 'pre') {
          yield vars.api.modules[mod]['module'](vars)
        }
      }
    }
    return Promise.resolve()
  })

  // Get array of documents that point to [parent]
  // TODO: Replace with aggregate 
  var _findNodes = co.wrap(function * (parent, edges) {

    // Function that processes either an $out or $in object
    var processPredicate = co.wrap(function * (myEdge, key) {
      var projectiondir = null, edgesCursor, totalcount, limitcount, projection

      // Start the cursor
      if (myEdge['$edgepipeline']) {
        // Aggregate

        var matchIndex = -1
        for (var i in myEdge['$edgepipeline']) {
          if (myEdge['$edgepipeline'][i] == '$match') {
            matchIndex = i
            break
          }
        }

        if (matchIndex == -1) {
          myEdge['$edgepipeline'].unshift({ '$match': {} })
          matchIndex = 0
        }

        myEdge['$edgepipeline'][matchIndex]['$match']['predicate'] = myEdge['$predicate'] || key

        edgesCursor = db.collection('_edges').aggregate(myEdge['$edgepipeline'], { cursor: { batchSize: 1 } })
      } else {
        // Find 

        // Build query
        var query = { predicate: myEdge['$predicate'] || key }
        query['edges.id'] = parent._id

        // Filter
        if (myEdge['$filter']) {
          query.docs = {
            '$elemMatch': {
              _id: { $ne: parent._id }
            }
          }
          for (var attrname in myEdge['$filter']) {
            query.docs['$elemMatch'][attrname] = myEdge['$filter'][attrname]
          }
        }
        edgesCursor = db.collection('_edges').find(query)
      }

      // Project (show/hide) fields    
      if (myEdge['$project']) {
        projection = {}
        for (var attrname in myEdge['$project']) {
          projection['docs.' + attrname] = myEdge['$project'][attrname]
          projectiondir = projectiondir || myEdge['$project'][attrname]
        }
        if (projectiondir) {
          if (projectiondir == 1) {
            projection['edges.id'] = 1
            projection['edges.col'] = 1
            projection['edges.vertice'] = 1
            projection['predicate'] = 1
            projection['data'] = 1
          }
        }
        edgesCursor.project(projection)
      }

      // Sorting
      if (myEdge['$sort']) {
        var sortobj = {}
        for (var sortkey in myEdge['$sort']) {
          sortobj['docs.' + sortkey] = myEdge['$sort'][sortkey]
        }
        edgesCursor = edgesCursor.sort(sortobj)
      }

      // Limits, sort, and skip
      if (myEdge['$limit']) edgesCursor = edgesCursor.limit(myEdge['$limit'])
      if (myEdge['$skip']) edgesCursor = edgesCursor.skip(myEdge['$skip'])

      // Get some totals    
      if (!myEdge['$edgepipeline']) {
        totalcount = yield edgesCursor.count(false)
        limitcount = yield edgesCursor.count(true)
      }

      // Transform the output    
      parent.nodes = parent.nodes || {}
      parent.nodes[key] = {
        totalcount: totalcount,
        limitcount: limitcount,
        predicate: key
      }

      // Iterate over the edges
      while (yield edgesCursor.hasNext()) {
        var node = yield edgesCursor.next()

        if (myEdge['$edgepipeline']) {
          parent.nodes[key].documents = parent.nodes[key].documents || []
          parent.nodes[key].documents.push(node)
        } else {

          // Recursively find more nodes deeper      
          for (var childkey in myEdge) {
            if (childkey.charAt(0) != '$') {
              yield processPredicate(myEdge[childkey], childkey)
            }
          }

          // Put the document in the group
          if (!myEdge['$countonly']) {
            parent.nodes[key].documents = parent.nodes[key].documents || []

            for (var i = 0; i < 2; i++) {
              if (node.docs[i]._id != parent._id) {
                node.docs[i]['_edge'] = {
                  _id: node._id,
                  predicate: key
                }
                var theotherIndex = Math.abs(i - 1)
                if (node.edges[theotherIndex].vertice) node.docs[i]['_edge'].thatVertice = node.edges[theotherIndex].vertice
                if (node.edges[i].vertice) node.docs[i]['_edge'].thisVertice = node.edges[i].vertice
                parent.nodes[key].documents.push(
                  node.docs[i]
                )
              }
            }
          }
        }
      }

      return Promise.resolve()
    })

    // Process the root groups
    for (var key in edges) {
      //console.log(edges[key])
      // process $required
      yield processPredicate(edges[key], key)
    }

    return Promise.resolve(parent)
  })

  // Load modules and possibly API from database  
  var _getAPI = co.wrap(function * () {

    // Load API from database  
    if (helpers.isString(vars.api)) {
      vars.api = yield db.collection('_apis').find({ 'code': vars.api }).limit(1).next()
      if (vars.api.doc) vars.api.doc = JSON.parse(vars.api.doc)
      if (vars.api.docs) vars.api.docs = JSON.parse(vars.api.docs)
      if (vars.api.pipeline) vars.api.pipeline = JSON.parse(vars.api.pipeline)
      if (vars.api.edge) vars.api.edge = JSON.parse(vars.api.edge)
      if (vars.api.edges) vars.api.edges = JSON.parse(vars.api.edges)
      if (vars.api.query) vars.api.query = JSON.parse(vars.api.query)
      if (vars.api.filter) vars.api.filter = JSON.parse(vars.api.filter)
      if (vars.api.project) vars.api.project = JSON.parse(vars.api.project)
      if (vars.api.replacement) vars.api.replacement = JSON.parse(vars.api.replacement)
      if (vars.api.update) vars.api.update = JSON.parse(vars.api.update)
      if (vars.api.options) vars.api.options = JSON.parse(vars.api.options)
      if (vars.api.sort) vars.api.sort = JSON.parse(vars.api.sort)
    }

    vars.api.globals = vars.globals

    // Preload module functions into the API  
    if (vars.api.modules) {
      for (var mod in vars.api.modules) {
        vars.api.modules[mod]['module'] = requireFromString(vars.api.modules[mod].code)
      }
    }

    return Promise.resolve()
  })
}

module.exports = {
  connect: function (db) {
    db.use = function (api) {
      return new EdgeClient(db, api)
    }
  }
}
