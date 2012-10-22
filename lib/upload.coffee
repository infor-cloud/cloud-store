fs = require "fs"
EventEmitter = require("events").EventEmitter
crypto = require "crypto"
cipherBlockSize = require 'cipher-block-size'
Stream = require 'stream'
knox = require 'knox'
Parser = require('node-expat').Parser
https = require 'https'

module.exports = (params) ->
  https.globalAgent.maxSockets = params['max-concurrent-connections'] if params['max-concurrent-connections']?

  blockSize = cipherBlockSize params.algorithm

  encKey = do ->
    keyFileName = params['enc-key-file'] or "#{process.env['HOME']}/.s3multipart-enc-keys"
    keyName = params['enc-key-name']
    new Buffer JSON.parse(fs.readFileSync(keyFileName, 'utf8'))[keyName], 'hex'

  client = knox.createClient key: params['aws-access-key'], secret: params['aws-secret-key'], bucket: params.bucket
  startMultipart = (tried) ->
    meta =
      algorithm: params.algorithm
      'chunk-size': params.chunkSize
      'enc-key-name': params['enc-key-name']
      'protocol-version': "0.0" #!!! Change to 1.0 before production
    headers = {}
    headers["x-amz-meta-s3multipart-#{key}"] = value for key, value of meta
    req = client.request 'POST', "/#{params.fileName}?uploads", headers
    req.on 'response', (res) ->
      if res.statusCode < 300
        parser = new Parser()
        uploadId = ""
        parser.on 'startElement', (name) ->
          if name.toLowerCase() is 'uploadid'
            parser.on 'text', (text) ->
              uploadId += text
            parser.on 'endElement', ->
              parser.removeAllListeners 'text'
        res.on 'end', ->
          startUpload uploadId
        res.pipe parser
      else
        console.error "Error: Response code #{res.statusCode}"
        console.error "Headers:"
        console.error require('util').inspect res.headers
        res.on 'data', (chunk) ->
          console.error chunk.toString()
        res.on 'end', ->
          process.exit 1 if tried
          startMultipart true

    req.end()

  startMultipart false

  startUpload = (uploadId) ->
    chunkCount = null
    completeReq = []
    encrypt = (stream) ->
      newStream = new Stream()
      newStream.readable = true
      newStream.errored = false
      newStream.pause = -> stream.pause.apply stream, arguments
      newStream.resume = -> stream.resume.apply stream, arguments
      newStream.destroy = -> stream.destroy.apply stream, arguments
      newStream.index = stream.index
      crypto.randomBytes blockSize, (err, iv) ->
        if err
          newStream.emit 'error', err
        else unless newStream.errored
          cipher = crypto.createCipheriv params.algorithm, encKey, iv
          newStream.emit 'data', iv
          newStream.emit 'data', cipher.update buf, 'buffer', 'buffer' for buf in stream.bufArray
          delete stream['bufArray']
          if stream?.gotEnd
            newStream.emit cipher.final 'buffer'
            newStream.readable = false
            newStream.emit 'end'
          else
            stream.removeAllListeners 'data'
            stream.removeAllListeners 'end'
            stream.resume()
            stream.on 'data', (data) ->
              newStream.emit 'data', cipher.update data, 'buffer', 'buffer'
            stream.on 'end', ->
              newStream.emit 'data', cipher.final 'buffer'
              newStream.readable = false
              newStream.emit 'end'
            stream.on 'close', ->
              newStream.emit 'close'
      stream.bufArray = []
      stream.pause()
      stream.on 'data', (chunk) ->
        stream.bufArray.push chunk
      stream.on 'end', ->
        stream.gotEnd = true
      stream.on 'error', (exception) ->
        unless newStream.errored
          newStream.errored = true
          newStream.readable = false
          newStream.emit 'error', exception
      newStream.on 'error', (exception) ->
        unless newStream.errored
          newStream.errored = true
          newStream.readable = false
          stream.emit 'error', exception
      newStream

    addHash = (stream) ->
      hash = crypto.createHash 'sha256'
      newStream = new Stream()
      newStream.readable = true
      newStream.errored = false
      newStream.pause = -> stream.pause.apply stream, arguments
      newStream.resume = -> stream.resume.apply stream, arguments
      newStream.destroy = -> stream.destroy.apply stream, arguments
      newStream.index = stream.index
      stream.on 'data', (data) ->
        newStream.emit 'data', data
        hash.update data, 'buffer'
      stream.on 'end', ->
        newStream.emit 'data', hash.digest 'buffer'
        newStream.readable = false
        newStream.emit 'end'
      stream.on 'error', (exception) ->
        unless newStream.errored
          newStream.errored = true
          newStream.readable = false
          newStream.emit 'error', exception
      newStream.on 'error', (exception) ->
        unless newStream.errored
          newStream.errored = true
          newStream.readable = false
          stream.emit 'error', exception
      stream.on 'close', ->
        newStream.emit 'close'
      newStream

    finalize = ->
      completeString = "<CompleteMultipartUpload>"
      completeString += reqPart for reqPart in completeReq
      completeString += "</CompleteMultipartUpload>"
      completeReq = (tried) ->
        req = client.request 'POST', "/#{params.fileName}?uploadId=#{uploadId}", 'Content-Length': completeString.length
        req.on 'response', (res) ->
          if res.statusCode < 300
            process.exit
          else
            console.error "Error: Response code #{res.statusCode}"
            console.error "Headers:"
            console.error require('util').inspect res.headers
            res.on 'data', (chunk) ->
              console.error chunk.toString()
            res.on 'end', ->
              process.exit 1 if tried
              completeReq true
        req.end completeString
      completeReq false

    upload = do ->
      streamLength = (Math.floor(params.chunkSize/blockSize) + 2) * blockSize + 32
      activeReqs = 0
      queuedReqs = []
      freeListeners = https.globalAgent.listeners('free').slice 0
      https.globalAgent.removeAllListeners 'free'
      removeActiveReq = ->
        activeReqs -= 1
        queued = queuedReqs.shift()
        if queued
          unless queued.errored
            queued.resume()
            upload queued
          else
            activeReqs += 1
            removeActiveReq()
      https.globalAgent.on 'free', removeActiveReq
      https.globalAgent.on 'free', listener for listener in freeListeners
      (stream) ->
        if activeReqs >= https.globalAgent.maxSockets
          stream.bufArray = []
          stream.pause()
          stream.on 'data', (chunk) ->
            stream.bufArray.push chunk
          stream.on 'end', ->
            stream.gotEnd = true
          queuedReqs.push stream
        else
          activeReqs += 1
          remoteHash = null
          localHash = null
          hashesDone = ->
            if localHash.equals remoteHash
              completeReq[stream.index] = "<Part><PartNumber>#{stream.index + 1}</PartNumber><ETag>#{remoteHash.toString 'hex'}</ETag></Part>"
              chunkCount -= 1
              if chunkCount is 0
                finalize()
            else
              req.emit 'error', "Bad hash"
          hash = crypto.createHash 'md5'
          req = client.put "/#{params.fileName}?partNumber=#{stream.index + 1}&uploadId=#{uploadId}",
            {'Content-Length': streamLength, Connection: 'keep-alive' }
          stream.req = req
          req.errored = false
          stream.on 'error', (err) ->
            unless req.errored
              req.errored = true
              req.emit 'error', err
              req.readable = false
          req.on 'error', (err) ->
            unless req.errored
              req.errored = true
              stream.emit 'error', err
              req.readable = false
            if req?.socket
              req.socket.emit 'agentRemove'
              req.socket.destroy()
              delete req.socket
            removeActiveReq()
          req.on 'socket', (socket) ->
            try
              socket.resume()
          req.on 'response', (res) ->
            if res.statusCode < 300
              res.on 'error', ->
              remoteHash = new Buffer(res.headers.etag.slice(1,33), 'hex')
              hashesDone() unless localHash is null
            else
              err = "Error: Response code #{res.statusCode}\n"
              err += "Headers:\n"
              err += require('util').inspect res.headers
              err += "\n"
              res.on 'data', (chunk) ->
                err += chunk.toString()
                err += "\n"
              res.on 'end', ->
                req.emit 'error', err
              res.on 'error', (err) ->
                req.emit 'error', err
          if stream?.bufArray
            for chunk in stream.bufArray
              req.write chunk
              hash.update chunk, 'buffer'
            delete stream['bufArray']
          if stream?.gotEnd
            req.end()
            localHash = hash.digest 'buffer'
          else
            stream.removeAllListeners 'data'
            stream.removeAllListeners 'end'
            stream.on 'data', (chunk) ->
              hash.update chunk, 'buffer'
            stream.on 'end', ->
              localHash = hash.digest 'buffer'
              hashesDone() unless remoteHash is null
            stream.pipe req

    createNewReadStream = ->

    fs.open params.file, 'r', (err, fd) ->
      throw err if err
      fs.fstat fd, (err, stats) ->
        throw err if err
        createNewReadStream = (pos) ->
          errored = false
          onerror = (err) ->
            return if errored
            errored = true
            console.error "Error in chunk #{pos / params.chunkSize}: #{err}"
            chunkCount -= 1
            createNewReadStream pos
          chunkCount += 1
          newStream = fs.createReadStream params.file,
            fd: fd,
            start: pos,
            end: Math.min(pos + params.chunkSize - 1, stats.size - 1)
          newStream.destroy = -> @readable = false
          newStream.index = pos / params.chunkSize
          if pos + params.chunkSize < stats.size
            newStream.on 'error', onerror
            upload addHash encrypt newStream
          else
            finalStream = new Stream()
            finalStream.pause = -> newStream.pause.apply newStream, arguments
            finalStream.resume = -> newStream.resume.apply newStream, arguments
            finalStream.destroy = -> newStream.destroy.apply newStream, arguments
            finalStream.index = newStream.index
            finalStream.readable = true
            newStream.on 'data', (data) ->
              finalStream.emit 'data', data
            newStream.on 'end', ->
              if finalStream.readable
                pad = new Buffer(params.chunkSize - (stats.size - pos))
                pad.fill 0
                finalStream.emit 'data', pad
                finalStream.readable = false
              finalStream.emit 'end'
            newStream.on 'error', (exception) ->
              finalStream.readable = false
              finalStream.emit 'error', exception
            newStream.on 'close', ->
              finalStream.readable = false
              finalStream.emit 'close'
            finalStream.on 'error', onerror
            upload addHash encrypt finalStream
        process.nextTick ->
          for pos in [0..stats.size - 1] by params.chunkSize
            createNewReadStream pos
