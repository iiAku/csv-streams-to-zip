const {PassThrough} = require('stream')
const AWS = require('aws-sdk')
const archiver = require('archiver')
const pump = require('pump')
const os = require('os')
require('dotenv').config()

const concatStreams = (streamArray, streamCounter = streamArray.length) =>
  streamArray.reduce((mergedStream, stream) => {
    mergedStream = stream.pipe(mergedStream, {end: false})
    stream.once('end', () => --streamCounter === 0 && mergedStream.emit('end'))
    return mergedStream
  }, new PassThrough())

const sendObject = (params, options) => {
  const s3 = new AWS.S3()
  if (!options) {
    return s3.upload(params).promise()
  }
  return s3.upload(params, options).promise()
}

const lineParser = (line, idx) => {
  //skip all next headers
  if (idx++ > 0 && line.indexOf('{app_id}') !== -1) {
    return ''
  }
  return line + os.EOL
}

const zipAndSend = async (
  inputStream,
  mergedCsvFilename,
  outputZipFilename
) => {
  const archive = archiver('zip', {
    zlib: {level: 9}
  })
  archive.append(inputStream, {name: mergedCsvFilename})
  const uploadStream = new PassThrough()
  pump(archive, uploadStream, () => {
    console.log('Pipe finished')
  })
  archive.finalize()
  archive.on('warning', function (err) {
    if (err.code === 'ENOENT') {
      console.log(err)
    } else {
      throw err
    }
  })
  archive.on('error', function (err) {
    throw err
  })
  archive.on('end', function () {
    console.log('Archive done')
  })
  // TODO - pass up params to parent function
  return sendObject(
    {
      Bucket: process.BUCKET_NAME,
      Key: outputZipFilename,
      Body: uploadStream,
      ContentType: 'application/zip'
    },
    {partSize: 10 * 1024 * 1024, queueSize: 5}
  )
}

module.exports = {concatStreams, lineParser, sendObject, zipAndSend}
