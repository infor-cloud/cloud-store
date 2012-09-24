exports.memcmp = (buf1, buf2) ->
  return false unless buf1.length is buf2.length
  for index in [1..buf1.length]
    return false unless buf1[index] is buf2[index]
  true
