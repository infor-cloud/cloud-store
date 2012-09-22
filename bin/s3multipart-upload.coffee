#!/usr/bin/coffee

fs = require "fs"
EventEmitter = require("events").EventEmitter
crypto = require "crypto"
cipherBlockSize = require 'cipher-block-size'
params = require('optimist').argv
Stream = require 'stream'
knox = require 'knox'
Parser = require('node-expat').Parser

memcmp = (buf1, buf2) ->
  return false unless buf1.length is buf2.length
  for index in [1..buf1.length]
      return false unless buf1[index] is buf2[index]
  true

client = knox.createClient key: params['aws-access-key'], secret: params['aws-secret-key'], bucket: params.bucket
req = client.request 'POST', "/#{params.fileName}?uploads"
req.on 'response', (res) ->
  if res.statusCode is 200
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
      process.exit 1

req.end()

startUpload = (uploadId) ->
  chunkCount = null
  completeReq = []
  encryptionFilter = (inStreamEmitter) ->
    outStreamEmitter = new EventEmitter()
    inStreamEmitter.on 'stream', (stream) ->
      cipher = crypto.createCipher params.algorithm, params.password
      newStream = new Stream()
      newStream.readable = true
      newStream.pause = -> stream.pause.call stream, arguments
      newStream.resume = -> stream.resume.call stream, arguments
      newStream.destroy = -> stream.destroy.call stream, arguments
      newStream.index = stream.index
      stream.on 'data', (data) ->
        newStream.emit 'data', cipher.update data, 'buffer', 'buffer'
      stream.on 'end', ->
        if newStream.readable
          newStream.emit 'data', cipher.final 'buffer'
          newStream.readable = false
        newStream.emit 'end'
      stream.on 'error', (exception) ->
        newStream.readable = false
        newStream.emit 'error', exception
      stream.on 'close', ->
        if newStream.readable
          newStream.emit 'data', cipher.final 'buffer'
          newStream.readable = false
        newStream.emit 'close'
      outStreamEmitter.emit 'stream', newStream
    outStreamEmitter

  hashFilter = (inStreamEmitter) ->
    outStreamEmitter = new EventEmitter()
    inStreamEmitter.on 'stream', (stream) ->
      hash = crypto.createHash 'sha256'
      newStream = new Stream()
      newStream.readable = true
      newStream.pause = -> stream.pause.call stream, arguments
      newStream.resume = -> stream.resume.call stream, arguments
      newStream.destroy = -> stream.destroy.call stream, arguments
      newStream.index = stream.index
      stream.on 'data', (data) ->
        newStream.emit 'data', data
        hash.update data, 'buffer'
      stream.on 'end', ->
        newStream.emit 'data', hash.digest 'buffer'
        newStream.readable = false
        newStream.emit 'end'
      stream.on 'error', (exception) ->
        newStream.readable = false
        newStream.emit 'error', exception
      stream.on 'close', ->
        newStream.emit 'close'
      outStreamEmitter.emit 'stream', newStream
    outStreamEmitter

  finalize = ->
    completeString = "<CompleteMultipartUpload>"
    completeString += reqPart for reqPart in completeReq
    completeString += "</CompleteMultipartUpload>"
    req = client.request 'POST', "/#{params.fileName}?uploadId=#{uploadId}", 'Content-Length': completeString.length
    req.on 'response', (res) ->
      console.log "Headers:"
      console.log require('util').inspect res.headers
      res.on 'data', (chunk) ->
        console.log chunk.toString()
      res.on 'end', ->
        process.exit
    req.end completeString

  upload = (inStreamEmitter) ->
    blockSize = cipherBlockSize params.algorithm
    streamLength = (Math.floor(params.chunkSize/blockSize) + 1) * blockSize + 32
    inStreamEmitter.on 'stream', (stream) ->
      remoteHash = null
      localHash = null
      hashesDone = ->
        throw "Bad hash" unless memcmp(localHash, remoteHash)
        completeReq[stream.index] = "<Part><PartNumber>#{stream.index + 1}</PartNumber><ETag>#{remoteHash.toString 'hex'}</ETag></Part>"
        chunkCount -= 1
        if chunkCount is 0
          finalize()
      hash = crypto.createHash 'md5'
      stream.on 'data', (chunk) ->
        hash.update chunk, 'buffer'
      stream.on 'end', ->
        localHash = hash.digest 'buffer'
        hashesDone() unless remoteHash is null
      client.putStream stream, "/#{params.fileName}?partNumber=#{stream.index + 1}&uploadId=#{uploadId}",
        {'Content-Length': streamLength}, (err, res) ->
          throw err if err
          if res.statusCode is 200
            remoteHash = new Buffer(res.headers.etag.slice(1,33), 'hex')
            hashesDone() unless localHash is null
          else
            console.error "Error: Response code #{res.statusCode}"
            console.error "Headers:"
            console.error require('util').inspect res.headers
            res.on 'data', (chunk) ->
              console.error chunk.toString()
            res.on 'end', ->
              process.exit 1

  fs.open params.file, 'r', (err, fd) ->
    throw err if err
    fs.fstat fd, (err, stats) ->
      throw err if err
      initialStreamEmitter = new EventEmitter()
      createNewReadStream = (pos) ->
        chunkCount += 1
        newStream = fs.createReadStream params.file,
          fd: fd,
          start: pos,
          end: Math.min(pos + params.chunkSize - 1, stats.size)
        newStream.destroy = -> @readable = false
        newStream.index = pos / params.chunkSize
        if pos + params.chunkSize < stats.size
          initialStreamEmitter.emit 'stream', newStream
          createNewReadStream pos + params.chunkSize
        else
          finalStream = new Stream()
          finalStream.pause = -> newStream.pause.call newStream, arguments
          finalStream.resume = -> newStream.resume.call newStream, arguments
          finalStream.destroy = -> newStream.destroy.call newStream, arguments
          finalStream.index = newStream.index
          newStream.on 'data', (data) ->
            finalStream.emit 'data', data
          newStream.on 'end', ->
            pad = new Buffer(params.chunkSize - (stats.size - pos))
            pad.fill 0
            finalStream.emit 'data', pad
            finalStream.readable = false
            finalStream.emit 'end'
          newStream.on 'error', (exception) ->
            finalStream.readable = false
            finalStream.emit 'error', exception
          newStream.on 'close', ->
            pad = new Buffer(params.chunkSize - (stats.size - pos))
            pad.fill 0
            finalStream.emit 'data', pad
            finalStream.readable = false
            finalStream.emit 'end'
          initialStreamEmitter.emit 'stream', finalStream
      process.nextTick ->
        createNewReadStream 0
      upload hashFilter encryptionFilter initialStreamEmitter
