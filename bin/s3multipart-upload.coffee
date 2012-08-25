#!/usr/bin/coffee

fs = require "fs"
EventEmitter = require("events").EventEmitter

compressionFilter = (inStreamEmitter) -> inStreamEmitter

encryptionFilter = (inStreamEmitter) -> inStreamEmitter

splitFilter = (inStreamEmitter) -> inStreamEmitter

sequentialRead = (fd) ->
  initialStreamEmitter = new EventEmitter()
  process.nextTick ->
    initialStreamEmitter.emit 'stream', fs.createReadStream null, fd: fd
  upload encryptionFilter compressionFilter splitFilter initialStreamEmitter

parallelRead = (fd) ->
  fs.fstat fd, (err, stats) ->
    initialStreamEmitter = new EventEmitter()
    createNewReadStream = (pos) ->
      newStream = fs.createReadStream null, fd: fd, start: pos, end: pos + chunkSize - 1
      newStream.on 'end', ->
        createNewReadStream pos + chunkSize
      initialStreamEmitter.emit 'stream', newStream
    process.nextTick ->
      createNewReadStream 0
    upload encryptionFilter compressionFilter initialStreamEmitter

fs.open fileName, 'r', (err, fd) ->
  throw err if err
  buf = new Buffer 1
  fs.read fd, buf, 0, 0, 0, (err) ->
    buf = null
    if err and err.code is 'ESPIPE'
      sequentialRead fd
    else
      throw err if err
      parallelRead fd
