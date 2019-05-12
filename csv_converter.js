const aws = require('aws-sdk')
const csv = require('fast-csv')
require('dotenv')
const fs = require('fs')
const request = require('request')
const stream = require('stream')
const utils = require('util')
const mkdir = utils.promisify(fs.mkdir)

if (!process.env.SECRET_ACCESS_KEY && !process.env.ACCESS_KEY_ID && !process.env.REGION)
    throw new Error("Internal server error")

aws.config.update({
    secretAccessKey: process.env.SECRET_ACCESS_KEY,
    accessKeyId: process.env.ACCESS_KEY_ID,
    region: process.env.REGION
})

const s3 = new aws.S3();

function errorHandler(error,statusCode, message) {
    console.log(error)
    return {
        statusCode: statusCode,
        body: JSON.stringify({
            message
        }),
    };
}

function successHandler(statusCode, body) {
    return {
        statusCode,
        body: JSON.stringify(body)
    }
}

function uploadFromStream(bucket, acl, key, cb) {
    var pass = new stream.PassThrough();
    var params = { Bucket: bucket, Key: key, acl, Body: pass };
    s3.upload(params, cb);

    return pass;
}


const convertAndUploadCsv = async (fileLink) => {
    return new Promise((resolve, reject) => {
        request(fileLink)
            .pipe(fs.createWriteStream('input.csv'))
            .on('finish', function(){
                const readStream = fs.createReadStream('./input.csv');
                let fileNameColumn = null
                let remainingColumns = null
                const fileMap = {}
                let filesUploaded = 0

                csv
                    .fromStream(readStream)
                    .on("data", function(row){
                        console.log(row)
                        if(!fileNameColumn && !remainingColumns){
                            fileNameColumn = row.shift()
                            remainingColumns = row
                        }
                        else {

                            const key = row.shift()
                            if(!fileMap[key]) {
                                fileMap[key] = [remainingColumns]
                            }
                            if(key)
                                fileMap[key].push(row)
                        }
                    })
                    .on("end", async function () {
                        delete fileMap["undefined"]
                        const files = Object.keys(fileMap)
                        files.forEach((key) => {
                            const newWriteStream = fs.createWriteStream(`${key}.csv`)

                            csv
                                .write(fileMap[key], { headers: true })
                                .pipe(newWriteStream)
                                .on('finish',async () => {
                                        const readStream = fs.createReadStream(`./${key}.csv`);
                                        readStream.pipe(
                                            uploadFromStream
                                            (
                                                'ticketlake-dev',
                                                "public-read",
                                                key + '.csv',
                                                function (err, data) {
                                                    filesUploaded++
                                                    if (err) {
                                                        errorHandler(err, 500, "Error occured uploading file to s3")
                                                    }
                                                    else {
                                                        if (filesUploaded === files.length) {
                                                            files.forEach((fileName) => {
                                                                fs.unlinkSync(`./${fileName}.csv`)
                                                            })
                                                            fs.unlinkSync('./input.csv')
                                                            resolve(successHandler(200, {
                                                                message: "Files Uploaded"
                                                            }))
                                                        }
                                                    }
                                                }
                                            )
                                        )
                                })
                                .on('error', (err) => reject(errorHandler(err, 404, "File not found")))
                        })
                    })
                    .on("error", (err) => reject(errorHandler(err, 404, "File not found")))
            })
    })

};


module.exports.csvConverter = async function(event, context, callback) {
    let body = null

    if(event.httpMethod === "POST" && event.body) {
        body = JSON.parse(event.body)
    }

    if(!body)
        return errorHandler("No body found", 404, "No body found")
    if(!body.fileLink)
        return errorHandler("Please provide link to a csv", 400, "Please provide link to a csv")

    return await convertAndUploadCsv(body.fileLink)

}

