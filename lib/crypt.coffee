Stream = require 'stream'
crypto = require 'crypto'

exports.encrypt = (stream, algorithm, blockSize, encKey) ->
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
      cipher = crypto.createCipheriv algorithm, encKey, iv
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
