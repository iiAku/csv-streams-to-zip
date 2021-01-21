const path = require('path')
const fs = require('fs')
const es = require('event-stream')
const {concatStreams, lineParser, zipAndSend} = require('./helpers')

const main = async () => {
  // Get csv list
  const csvBasePath = path.join(__dirname, 'csv')
  const csvPaths = fs
    .readdirSync(csvBasePath)
    .map((csv) => path.join(csvBasePath, csv))

  const idx = 0
  // Turn csvs into a single readStream
  const readStreams = concatStreams(
    csvPaths.map((csvPath) => {
      return fs.createReadStream(csvPath)
    })
  )
    .pipe(es.split())
    .pipe(es.mapSync((line) => lineParser(line, idx)))

  // Zip that stream out and put it on S3
  await zipAndSend(readStreams, 'merged.csv', 'merged.zip')
  console.log('Zip has been sent properly')
}

main()
