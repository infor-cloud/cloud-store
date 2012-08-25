#!/usr/bin/coffee

fs = require "fs"
EventEmitter = require("events").EventEmitter

compressionFilter = (inStreamEmitter) -> inStreamEmitter

encryptionFilter = (inStreamEmitter) -> inStreamEmitter

splitFilter = (inStreamEmitter) ->
  outStreamEmitter = new EventEmitter()
  inStreamEmitter.on 'stream', (stream) ->
    newStream = new Stream()
    newStream.readable = true
    newStream.count = 0
    newStream.index = stream.index.append [ 0 ]
    newStream.pause = stream.pause
    newStream.resume = stream.resume
    newStream.destroy = stream.destroy
    stream.on 'data', (data) ->
      if data.length + newStream.count <= chunkSize
        newStream.count += data.length
        newStream.emit 'data', data
      else
        oldCount = newStream.count
        newStream.emit 'data', data.slice 0, chunkSize - oldCount
        index = newStream.index.pop() + 1
        newStream.readable = false
        newStream.emit 'end'
        newStream = new Stream()
        newStream.readable = true
        newStream.index = stream.index.append [ index ]
        newStream.count = data.length - (chunkSize - oldCount)
        newStream.pause = stream.pause
        newStream.resume = stream.resume
        newStream.destroy = stream.destroy
        outStreamEmitter.emit 'stream', newStream
        newStream.emit 'data', data.slice chunkSize - count
    stream.on 'end', ->
      newStream.readable = false
      newStream.emit 'end'
      outStreamEmitter.emit 'lastIndex', newStream.index
    stream.on 'error', (exception) ->
      newStream.readable = false
      newStream.emit 'error', exception
    stream.on 'close', ->
      newStream.readable = false
      newStream.emit 'close'
    outStreamEmitter.emit 'stream', newStream
  outStreamEmitter

sequentialRead = (fd) ->
  initialStreamEmitter = new EventEmitter()
  process.nextTick ->
    stream = fs.createReadStream null, fd: fd
    stream.index = [ 0 ]
    initialStreamEmitter.emit 'stream', stream
    initialStreamEmitter.emit 'lastIndex', stream.index
  upload encryptionFilter compressionFilter splitFilter initialStreamEmitter

parallelRead = (fd) ->
  fs.fstat fd, (err, stats) ->
    initialStreamEmitter = new EventEmitter()
    createNewReadStream = (pos) ->
      newStream = fs.createReadStream null,
        fd: fd,
        start: pos,
        end: Math.max pos + chunkSize - 1, stats.size
      newStream.index = [ pos / chunkSize ]
      initialStreamEmitter.emit 'stream', newStream
      if pos + chunkSize < stats.size
        createNewReadStream pos + chunkSize
      else
        initialStreamEmitter.emit 'lastIndex', newStream.index
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
