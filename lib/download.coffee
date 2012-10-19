#!/usr/bin/coffee

fs = require 'fs'
EventEmitter = require('events').EventEmitter
crypto = require 'crypto'
cipherBlockSize = require 'cipher-block-size'
params = require('optimist').argv
Stream = require 'stream'
knox = require 'knox'
util = require './util'
https = require 'https'

maxProtocolVersion = 0.0

streamLength = undefined
blockSize = undefined
encKey = undefined

client = knox.createClient key: params['aws-access-key'], secret: params['aws-secret-key'], bucket: params.bucket

https.globalAgent.maxSockets = params['max-concurrent-connections'] if params['max-concurrent-connections']?

removeAndCheckHashFilter = (inStreamEmitter) ->
  outStreamEmitter = new EventEmitter()
  inStreamEmitter.on 'stream', (stream) ->
    hash = crypto.createHash 'sha256'
    newStream = new Stream()
    newStream.readable = true
    newStream.pause = -> stream.pause.apply stream, arguments
    newStream.resume = -> stream.resume.apply stream, arguments
    newStream.destroy = -> stream.destroy.apply stream, arguments
    newStream.index = stream.index
    bytesLeft = streamLength
    hashBuf = new Buffer(32)
    hashBufOffset = 0
    stream.on 'data', (data) ->
      bytesLeft -= data.length
      if bytesLeft <= 0
        dataByteCount = Math.max data.length + bytesLeft, 0
        data.copy hashBuf, hashBufOffset, dataByteCount
        hashBufOffset += data.length - dataByteCount
        data = data.slice 0, dataByteCount
      hash.update data, 'buffer'
      newStream.emit 'data', data
    stream.on 'end', ->
      newStream.readable = false
      if util.memcmp(hashBuf, hash.digest('buffer'))
        newStream.emit 'end'
      else
        newStream.readable = false
        newStream.emit 'error', 'Bad hash'
    stream.on 'error', (exception) ->
      newStream.readable = false
      newStream.emit 'error', exception
    stream.on 'close', ->
      newStream.emit 'close'
    outStreamEmitter.emit 'stream', newStream
  outStreamEmitter

decryptionFilter = (inStreamEmitter) ->
  outStreamEmitter = new EventEmitter()
  inStreamEmitter.on 'stream', (stream) ->
    ivBufs = []
    ivLen = 0
    newStream = new Stream()
    newStream.readable = true
    newStream.pause = -> stream.pause.apply stream, arguments
    newStream.resume = -> stream.resume.apply stream, arguments
    newStream.destroy = -> stream.destroy.apply stream, arguments
    newStream.index = stream.index
    stream.on 'data', (data) ->
      ivPartSize = Math.min(blockSize - ivLen, data.length)
      ivBufs.push data.slice 0, ivPartSize
      ivLen += ivPartSize
      if ivLen is blockSize
        stream.removeAllListeners 'data'
        iv = Buffer.concat ivBufs, ivLen
        ivBufs = undefined
        decipher = crypto.createDecipheriv params.algorithm, encKey, iv
        newStream.emit 'data', decipher.update data.slice(ivPartSize), 'buffer', 'buffer'
        stream.on 'data', (data) ->
          newStream.emit 'data', decipher.update data, 'buffer', 'buffer'
        stream.on 'end', ->
          newStream.emit 'data', decipher.final 'buffer'
          newStream.readable = false
          newStream.emit 'end'
    stream.on 'error', (exception) ->
      newStream.readable = false
      newStream.emit 'error', exception
    stream.on 'close', ->
      newStream.readable = false
      newStream.emit 'close'
    outStreamEmitter.emit 'stream', newStream
  outStreamEmitter

save = (inStreamEmitter) ->
  fd = fs.openSync params.file, 'w'
  inStreamEmitter.on 'stream', (stream) ->
    errored = false
    onerror = (err) ->
      return if errored
      errored = true
      console.error "Error in chunk #{stream.index}: #{err}"
      createNewReadStream stream.index * (streamLength + 32)
    stream.on 'error', onerror
    write = fs.createWriteStream params.file, fd: fd, flags: 'w', start: (stream.index * params.chunkSize)
    write.destroy = -> @writeable = false
    write.end = (data, encoding, cb) ->
      if typeof data is 'function'
        cb = data
      else if typeof encoding is 'function'
        cb = encoding
        this.write data
      else if arguments.length > 0
        this.write data, encoding
      this.writable = false
      this.flush()
      cb(null) if cb
    write.on 'error', onerror
    stream.pipe write

createNewReadStream = ->

parseMeta = (meta) ->
  version = parseFloat meta['protocol-version']
  if version > maxProtocolVersion
    throw "This version of the upload protocol not supported"
  params.chunkSize = parseInt meta['chunk-size']
  params.algorithm = meta['algorithm']
  blockSize = cipherBlockSize params.algorithm
  streamLength = (Math.floor(params.chunkSize/blockSize) + 2) * blockSize
  encKey = do ->
    keyFileName = params['enc-key-file'] or "#{process.env['HOME']}/.s3multipart-enc-keys"
    keyName = meta['enc-key-name']
    new Buffer JSON.parse(fs.readFileSync(keyFileName, 'utf8'))[keyName], 'hex'

startMultipart = (tried) ->
  client.head("/#{params.fileName}").on('response', (res) ->
    if res.statusCode < 300
      fileLength = parseInt res.headers['content-length']
      meta = {}
      for key, value of res.headers
        meta[key.slice("x-amz-meta-s3multipart-".length)] = value if key.indexOf("x-amz-meta-s3multipart-") is 0
      parseMeta meta
      initialStreamEmitter = new EventEmitter()
      activeReqs = 0
      queuedReqs = []
      freeListeners = https.globalAgent.listeners('free').slice 0
      https.globalAgent.removeAllListeners 'free'
      removeActiveReq = ->
        activeReqs -= 1
        queued = queuedReqs.shift()
        if queued
          createNewReadStream queued
      https.globalAgent.on 'free', removeActiveReq
      https.globalAgent.on 'free', listener for listener in freeListeners
      createNewReadStream = (pos) ->
        req = null
        index = pos / (streamLength + 32)
        errored = false
        onerror = (err) ->
          return if errored
          errored = true
          console.error "Error in chunk #{index}: #{err}"
          if req?.socket
            req.socket.emit 'agentRemove'
            req.socket.destroy()
            removeActiveReq()
          createNewReadStream pos
        if activeReqs >= https.globalAgent.maxSockets
          queuedReqs.push pos
        else
          activeReqs += 1
          req = client.get "/#{params.fileName}",
            Range: "bytes=#{pos}-#{pos + (streamLength + 32) - 1}",
            Connection: 'keep-alive'
          req.on 'error', onerror
          req.on 'response', (res)->
            if res.statusCode < 300
              res.index = index
              initialStreamEmitter.emit 'stream', res
            else
              err = "Error: Response code #{res.statusCode}\n"
              err += "Headers:\n"
              err += require('util').inspect res.headers
              err += "\n"
              res.on 'data', (chunk) ->
                err += chunk.toString()
                err += "\n"
              res.on 'end', ->
                onerror err
              res.on 'error', onerror
          req.on 'socket', (socket) ->
            req.socket = socket
            socket.removeAllListeners 'error'
            socket.on 'error', onerror
            socket.resume()
          req.end()
      for pos in [0..fileLength - 1] by streamLength + 32
        createNewReadStream pos
      save decryptionFilter removeAndCheckHashFilter initialStreamEmitter
    else
      console.error "Error: Response code #{res.statusCode}"
      console.error "Headers:"
      console.error require('util').inspect res.headers
      res.on 'data', (chunk) ->
        console.error chunk.toString()
      res.on 'end', ->
        process.exit 1 if tried
        startMultipart true
  ).end()

startMultipart false
