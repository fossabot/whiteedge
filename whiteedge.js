'use strict'

// Modules
var Promise = require('bluebird'),
  co = require('co'),
  lib = require('./lib/_lib'),
  helpers = require('./lib/_helpers')

// Client
function EdgeClient (db) {
  var vars = {
    api: null, // api document
    query: null, // MongoDB parameter
    doc: null, // MongoDB parameter
    docs: null, // MongoDB parameter
    filter: null, // MongoDB parameter
    update: null, // MongoDB parameter
    replacement: null, // MongoDB parameter
    pipeline: null, // MongoDB options
    sort: null, // MongoDB options
    project: null, // MongoDB options
    limit: null, // MongoDB options
    skip: null, // MongoDB options
    edges: null, // MongoDB options
    options: null, // MongoDB options
    collection: null // MongoDB collection
  }

  /**
   * Private instance methods.
   * These methods have access to privat instance variables and methods.
   */

  // Get array of documents that point to [parent]
  // TODO: Replace with aggregate 
  var findNodes = co.wrap(function * (parent, edges) {

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
      yield processPredicate(edges[key], key)
    }

    return Promise.resolve(parent)
  })

  /**
   * Public instance methods.
   * These methods have access to private instance variables and methods.
   */

  // Starting point for all api calls    
  this.use = function (api) {
    if (helpers.isString(api)) {
      co(function * () {
        vars.api = yield db.collection('_apis').find({'code': params.use}).limit(1).next()
      })
    } else {
      vars.api = api
    }
    return this
  }

  // Set collection    
  this.collection = function (collection) {
    vars.collection = collection
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

  // edgeUpsertOne  
  this.edgeUpsertOne = co.wrap(function * (edge, update) {
    var results

    vars.api = yield lib.replaceGlobals(vars.api)
    edge.docs[0] = yield lib.inspectObject(vars.api.edge.docs[0], edge.docs[0])
    edge.docs[1] = yield lib.inspectObject(vars.api.edge.docs[1], edge.docs[1])
    edge.predicate = vars.api.edge.predicate
    edge.data = yield lib.inspectObject(vars.api.data, edge.data)
    edge = yield lib.checkEdge(edge)

    var query = {
      predicate: edge.predicate,
      edges: { $all: edge.docs }
    }
    // Clean up the query object, in case client has added extra attributes to the edge object.
    var results = yield db.collection('_edges').find(query).limit(1).next()

    if (!results) {
      results = yield this.edgeInsertOne(edge)
    }

    // Move the scope of the fields in the update to 'data'
    for (var op in update) {
      for (var field in update[op]) {
        update[op]['data.' + field] = update[op][field]
        delete (update[op][field])
      }
    }
    yield db.collection('_edges').findOneAndUpdate({
      _id: results._id
    }, update)

    return Promise.resolve(results)
  })

  // edgeDeleteOne  
  this.edgeDeleteOne = co.wrap(function * (edge) {
    vars.api = yield lib.replaceGlobals(vars.api)
    edge.predicate = vars.api.edge.predicate
    edge.docs[0] = yield lib.inspectObject(vars.api.edge.docs[0], edge.docs[0])
    edge.docs[1] = yield lib.inspectObject(vars.api.edge.docs[1], edge.docs[1])
    edge = yield lib.checkEdge(edge)

    var query = {
      predicate: edge.predicate,
      edges: { $all: edge.docs }
    }

    var results = yield db.collection('_edges').findOneAndDelete(query)
    return Promise.resolve(results.value._id)
  })

  // edgeInsertOne  
  this.edgeInsertOne = co.wrap(function * (edge) {
    vars.api = yield lib.replaceGlobals(vars.api)
    edge.docs[0] = yield lib.inspectObject(vars.api.edge.docs[0], edge.docs[0])
    edge.docs[1] = yield lib.inspectObject(vars.api.edge.docs[1], edge.docs[1])
    edge.predicate = vars.api.edge.predicate
    edge.data = yield lib.inspectObject(vars.api.data, edge.data)
    edge = yield lib.checkEdge(edge)

    var query = {
      predicate: edge.predicate,
      edges: { $all: edge.docs }
    }
    var results = yield db.collection('_edges').find(query).limit(1).next()

    if (results) {
      return Promise.resolve(results)
    } else {
      var cleanedge = {
        _id: helpers.getUUID(),
        predicate: edge.predicate,
        edges: edge.docs
      }
      if (edge.data) cleanedge.data = edge.data

      // Get source and target documents
      cleanedge.docs = []
      cleanedge.docs[0] = yield db.collection(cleanedge.edges[0].col).find({_id: cleanedge.edges[0].id}).limit(1).next()
      cleanedge.docs[1] = yield db.collection(cleanedge.edges[1].col).find({_id: cleanedge.edges[1].id}).limit(1).next()

      if (!cleanedge.docs[0]) return Promise.reject('A document specified in [edge] was not found. _id: ' + cleanedge.edges[0].id)
      if (!cleanedge.docs[1]) return Promise.reject('A document specified in [edge] was not found. _id: ' + cleanedge.edges[1].id)

      yield db.collection('_edges').findOneAndUpdate(query, cleanedge, { upsert: true })
      return Promise.resolve(cleanedge)
    }
  })

  // aggregate  
  this.aggregate = co.wrap(function * (pipeline, options) {
    if (!vars.collection) throw new Error('Collection parameter is missing.')
    vars.api = yield lib.replaceGlobals(vars.api)
    options = yield lib.inspectObject(vars.api.options, options)
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
              pipeline[i2] = yield lib.inspectObject(vars.api.pipeline[i], pipeline[i2])
              break
            }
          }
        }
      }
    } else {
      pipeline = vars.api.pipeline
    }
    var results = yield db.collection(vars.collection).aggregate(pipeline, options).toArray()
    return Promise.resolve(results)
  })
  // insertOne  
  this.insertOne = co.wrap(function * (doc, options) {
    if (!vars.collection) throw new Error('Collection parameter is missing.')
    vars.api = yield lib.replaceGlobals(vars.api)
    options = yield lib.inspectObject(vars.api.options, options)
    if (!vars.api.doc) throw new Error('API: insertOne requires a [doc] element, which was not supplied in the API.')
    doc = yield lib.inspectObject(vars.api.doc, doc)
    doc._id = doc._id || helpers.getUUID()
    yield db.collection(vars.collection).insertOne(doc, options)
    return Promise.resolve(doc)
  })
  // insertMany  
  this.insertMany = co.wrap(function * (docs, options) {
    if (!vars.collection) throw new Error('Collection parameter is missing.')
    vars.api = yield lib.replaceGlobals(vars.api)
    options = yield lib.inspectObject(vars.api.options, options)
    if (!vars.api.doc) throw new Error('API: insertMany requires a [doc] element, which was not supplied in the API.')
    for (var i = 0; i < docs.length; i++) {
      docs[i] = yield lib.inspectObject(vars.api.doc, docs[i])
      docs[i]._id = docs[i]._id || helpers.getUUID()
    }
    var results = yield db.collection(vars.collection).insertMany(docs, options)
    return Promise.resolve(results)
  })

  // replaceOne  
  this.replaceOne = co.wrap(function * (filter, doc, options) {
    if (!vars.collection) throw new Error('Collection parameter is missing.')
    vars.api = yield lib.replaceGlobals(vars.api)
    options = yield lib.inspectObject(vars.api.options, options)
    if (!vars.api.filter) throw new Error('API: replaceOne requires a [filter] element, which was not supplied in the API.')
    if (!vars.api.doc) throw new Error('API: replaceOne requires a [doc] element, which was not supplied in the API.')
    filter = yield lib.inspectObject(vars.api.filter, filter)
    doc = yield lib.inspectObject(vars.api.doc, doc)
    var results = yield db.collection(vars.collection).replaceOne(filter, doc, options)

    // Update edges        
    if (results.result.nModified > 0) {
      var edgeFilter, setOp
      for (var i = 0; i < 2; i++) {
        edgeFilter = {}, setOp = { $set: {} }
        for (var op in filter) {
          edgeFilter['docs.' + i + '.' + op] = filter[op]
        }
        setOp['$set']['docs.' + i] = doc
        yield db.collection('_edges').updateMany(edgeFilter, setOp)
      }
    }

    return Promise.resolve(results)
  })

  // updateMany  
  this.updateMany = co.wrap(function * (filter, update, options) {
    if (!vars.collection) throw new Error('Collection parameter is missing.')
    vars.api = yield lib.replaceGlobals(vars.api)
    options = yield lib.inspectObject(vars.api.options, options)
    if (!vars.api.filter) throw new Error('API: updateMany requires a [filter] element, which was not supplied in the API.')
    if (!vars.api.update) throw new Error('API: updateMany requires a [update] element, which was not supplied in the API.')
    filter = yield lib.inspectObject(vars.api.filter, filter)
    update = yield lib.inspectObject(vars.api.update, update)

    // Update all the documents        
    var results = yield db.collection(vars.collection).updateMany(filter, update, options)

    // Update edges        
    if (results.result.nModified > 0) {
      var edgeFilter, edgeUpdate
      for (var i = 0; i < 2; i++) {
        edgeFilter = {}, edgeUpdate = {}
        for (var op in filter) {
          edgeFilter['docs.' + i + '.' + op] = filter[op]
        }
        for (var op in update) {
          edgeUpdate[op] = {}
          for (var field in update[op]) {
            edgeUpdate[op]['docs.' + i + '.' + field] = update[op][field]
          }
        }
        yield db.collection('_edges').updateMany(edgeFilter, edgeUpdate)
      }
    }

    return Promise.resolve(results)
  })

  // updateOne  
  this.updateOne = co.wrap(function * (filter, update, options) {
    if (!vars.collection) throw new Error('Collection parameter is missing.')
    vars.api = yield lib.replaceGlobals(vars.api)
    options = yield lib.inspectObject(vars.api.options, options)
    if (!vars.api.filter) throw new Error('API: updateOne requires a [filter] element, which was not supplied in the API.')
    if (!vars.api.update) throw new Error('API: updateOne requires a [update] element, which was not supplied in the API.')
    filter = yield lib.inspectObject(vars.api.filter, filter)
    update = yield lib.inspectObject(vars.api.update, update)

    // Update the document        
    var results = yield db.collection(vars.collection).updateOne(filter, update, options)

    // Update edges        
    if (results.result.nModified > 0) {
      var edgeFilter, edgeUpdate
      for (var i = 0; i < 2; i++) {
        edgeFilter = {}, edgeUpdate = {}
        for (var op in filter) {
          edgeFilter['docs.' + i + '.' + op] = filter[op]
        }
        for (var op in update) {
          edgeUpdate[op] = {}
          for (var field in update[op]) {
            edgeUpdate[op]['docs.' + i + '.' + field] = update[op][field]
          }
        }
        yield db.collection('_edges').updateMany(edgeFilter, edgeUpdate)
      }
    }

    return Promise.resolve(results)
  })

  // findOneAndUpdate  
  this.findOneAndUpdate = co.wrap(function * (filter, update, options) {
    if (!vars.collection) throw new Error('Collection parameter is missing.')
    vars.api = yield lib.replaceGlobals(vars.api)
    options = yield lib.inspectObject(vars.api.options, options)
    if (!vars.api.filter) throw new Error('API: findOneAndUpdate requires a [filter] element, which was not supplied in the API.')
    if (!vars.api.update) throw new Error('API: findOneAndUpdate requires a [update] element, which was not supplied in the API.')
    filter = yield lib.inspectObject(vars.api.filter, filter)
    update = yield lib.inspectObject(vars.api.update, update)

    // Update the document        
    var results = yield db.collection(vars.collection).findOneAndUpdate(filter, update, options)

    // Update edges
    if (results.ok == 1) {
      var edgeFilter, edgeUpdate
      for (var i = 0; i < 2; i++) {
        edgeFilter = {}, edgeUpdate = {}
        for (var op in filter) {
          edgeFilter['docs.' + i + '.' + op] = filter[op]
        }
        for (var op in update) {
          edgeUpdate[op] = {}
          for (var field in update[op]) {
            edgeUpdate[op]['docs.' + i + '.' + field] = update[op][field]
          }
        }
        yield db.collection('_edges').updateMany(edgeFilter, edgeUpdate)
      }
    }

    return Promise.resolve(results)
  })

  // findOneAndDelete  
  this.findOneAndDelete = co.wrap(function * (filter, options) {
    if (!vars.collection) throw new Error('Collection parameter is missing.')
    vars.api = yield lib.replaceGlobals(vars.api)
    options = yield lib.inspectObject(vars.api.options, options)
    if (!vars.api.filter) throw new Error('API: findOneAndDelete requires a [filter] element, which was not supplied in the API.')
    filter = yield lib.inspectObject(vars.api.filter, filter)

    // Update the document        
    var results = yield db.collection(vars.collection).findOneAndDelete(filter, options)

    // Update edges
    if (results.lastErrorObject.n == 1) {
      yield db.collection('_edges').deleteMany({ 'edges.id': results.value._id })
    }

    return Promise.resolve(results)
  })

  // deleteOne  
  this.deleteOne = co.wrap(function * (filter, options) {
    if (!vars.collection) throw new Error('Collection parameter is missing.')
    vars.api = yield lib.replaceGlobals(vars.api)
    options = yield lib.inspectObject(vars.api.options, options)
    if (!vars.api.filter) throw new Error('API: deleteOne requires a [filter] element, which was not supplied in the API.')
    filter = yield lib.inspectObject(vars.api.filter, filter)
    var results = yield db.collection(vars.collection).deleteOne(filter, options)

    // Update edges
    if (results.deletedCount == 1) {
      var edgeFilter
      for (var i = 0; i < 2; i++) {
        edgeFilter = {}
        for (var op in filter) {
          edgeFilter['docs.' + i + '.' + op] = filter[op]
        }
        yield db.collection('_edges').deleteMany(edgeFilter)
      }
    }

    return Promise.resolve(results)
  })

  // deleteMany  
  this.deleteMany = co.wrap(function * (filter, options) {
    if (!vars.collection) throw new Error('Collection parameter is missing.')
    vars.api = yield lib.replaceGlobals(vars.api)
    options = yield lib.inspectObject(vars.api.options, options)
    if (!vars.api.filter) throw new Error('API: deleteMany requires a [filter] element, which was not supplied in the API.')
    filter = yield lib.inspectObject(vars.api.filter, filter)
    var results = yield db.collection(vars.collection).deleteMany(filter, options)

    // Update edges        
    if (results.result.nModified > 0) {
      var edgeFilter
      for (var i = 0; i < 2; i++) {
        edgeFilter = {}
        for (var op in filter) {
          edgeFilter['docs.' + i + '.' + op] = filter[op]
        }
        yield db.collection('_edges').deleteMany(edgeFilter)
      }
    }

    return Promise.resolve(results)
  })

  // findOneAndReplace  
  this.findOneAndReplace = co.wrap(function * (filter, replacement, options) {
    if (!vars.collection) throw new Error('Collection parameter is missing.')
    vars.api = yield lib.replaceGlobals(vars.api)
    options = yield lib.inspectObject(vars.api.options, options)
    if (!vars.api.filter) throw new Error('API: findOneAndReplace requires a [filter] element, which was not supplied in the API.')
    if (!vars.api.replacement) throw new Error('API: findOneAndReplace requires a [replacement] element, which was not supplied in the API.')
    filter = yield lib.inspectObject(vars.api.filter, filter)
    replacement = yield lib.inspectObject(vars.api.replacement, replacement)

    // Update the document        
    var results = yield db.collection(vars.collection).findOneAndReplace(filter, replacement, options)

    // Update edges        
    if (results.ok == 1) {
      yield db.collection('_edges').updateMany({ 'edges.0.id': results.value._id }, { $set: { 'docs.0': replacement }})
      yield db.collection('_edges').updateMany({ 'edges.1.id': results.value._id }, { $set: { 'docs.1': replacement }})
    }

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
    if (vars.type == 'mongo') {
      if (!vars.collection) throw new Error('Collection parameter is missing.')
      vars.api = yield lib.replaceGlobals(vars.api)
      vars.options = yield lib.inspectObject(vars.api.options, vars.options)
    }

    switch (vars.method) {
      case 'find':

        vars.query = yield lib.inspectObject(vars.api.query, vars.query)
        vars.sort = yield lib.inspectObject(vars.api.sort, vars.sort)
        vars.project = yield lib.inspectObject(vars.api.project, vars.project)
        vars.limit = yield lib.inspectInput('Limit', vars.api.limit, vars.limit, false)
        vars.skip = yield lib.inspectInput('Skip', vars.api.skip, vars.skip, true)

        
        var cursor = db.collection(vars.collection).find(vars.query)
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
            results.map(result => findNodes(result, vars.api.edges).then(parent => {
              // Add any data to the parent
              return parent
            }))
          )

          results = {
            documents: newresults,
            limitcount: limitcount,
            totalcount: totalcount
          }
          return Promise.resolve(results)
        } else {
          results = {
            documents: [],
            cursorcount: 0
          }
          return Promise.resolve(results)
        }

      default:
        throw new Error('Method not supported: ' + vars.method)
    }
  })
}

module.exports = {
  connect: co.wrap(function * (db) {
    db.EdgeClient = new EdgeClient(db)

    db.use = function (api) {
      return db.EdgeClient.use(api)
    }

    return Promise.resolve()
  })
}
