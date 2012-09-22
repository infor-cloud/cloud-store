#!/usr/bin/coffee

fs = require 'fs'
EventEmitter = require('events').EventEmitter
crypto = require 'crypto'
cipherBlockSize = require 'cipher-block-size'
params = require('optimist').argv
Stream = require 'stream'
knox = require 'knox'

blockSize = cipherBlockSize params.algorithm
streamLength = (Math.floor(params.chunkSize/blockSize) + 1) * blockSize

memcmp = (buf1, buf2) ->
  return false unless buf1.length is buf2.length
  for index in [1..buf1.length]
      return false unless buf1[index] is buf2[index]
  true

client = knox.createClient key: params['aws-access-key'], secret: params['aws-secret-key'], bucket: params.bucket

removeAndCheckHashFilter = (inStreamEmitter) ->
  outStreamEmitter = new EventEmitter()
  inStreamEmitter.on 'stream', (stream) ->
    hash = crypto.createHash 'sha256'
    newStream = new Stream()
    newStream.readable = true
    newStream.pause = -> stream.pause.call stream, arguments
    newStream.resume = -> stream.resume.call stream, arguments
    newStream.destroy = -> stream.destroy.call stream, arguments
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
      throw 'bad hash' unless memcmp(hashBuf, hash.digest('buffer'))
      newStream.emit 'end'
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
    decipher = crypto.createDecipher params.algorithm, params.password
    newStream = new Stream()
    newStream.readable = true
    newStream.pause = -> stream.pause.call stream, arguments
    newStream.resume = -> stream.resume.call stream, arguments
    newStream.destroy = -> stream.destroy.call stream, arguments
    newStream.index = stream.index
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
      newStream.emit 'close'
    outStreamEmitter.emit 'stream', newStream
  outStreamEmitter

save = (inStreamEmitter) ->
  fd = fs.openSync params.file, 'w'
  inStreamEmitter.on 'stream', (stream) ->
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
    stream.pipe write

client.head("/#{params.fileName}").on('response', (res) ->
  if res.statusCode is 200
    fileLength = parseInt res.headers['content-length']
    initialStreamEmitter = new EventEmitter()
    createNewReadStream = (pos) ->
      req = client.get "/#{params.fileName}",
        Range: "bytes=#{pos}-#{pos + (streamLength + 32) - 1}",
        Connection: 'keep-alive'
      req.on 'response', (res)->
        if res.statusCode < 300
          res.index = pos / (streamLength + 32)
          initialStreamEmitter.emit 'stream', res
        else
          console.error "Error: Response code #{res.statusCode}"
          console.error "Headers:"
          console.error require('util').inspect res.headers
          res.on 'data', (chunk) ->
            console.error chunk.toString()
          res.on 'end', ->
            process.exit 1
      req.on 'socket', (socket) ->
        socket.resume()
      req.end()
      if pos + (streamLength + 32) < fileLength
        createNewReadStream pos + (streamLength + 32)
    createNewReadStream 0
    save decryptionFilter removeAndCheckHashFilter initialStreamEmitter
  else
    console.error "Error: Response code #{res.statusCode}"
    console.error "Headers:"
    console.error require('util').inspect res.headers
    res.on 'data', (chunk) ->
      console.error chunk.toString()
    res.on 'end', ->
      process.exit 1
).end()
