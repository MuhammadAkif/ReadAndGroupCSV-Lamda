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


const promisifyReadStream = function(stream) {
    let fileNameColumn = null
    let remainingColumns = null
    const fileMap = {}

    return new Promise((resolve, reject) => {
        stream.on('data', function (row) {
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
        stream.on("finish", () => resolve(fileMap))
        stream.on("error", reject)

    })
}

const promisifyWriteStream = function (csvStream, writeStream) {
    const pipedStream = csvStream.pipe(writeStream)
    return new Promise((resolve, reject) => {
        pipedStream
            .on('finish', () => {
                resolve(true)
            })
            .on('error', reject)

    })

}

const uploadFileToS3 = function(files) {
    return new Promise((resolve, reject) => {
        let filesUploaded = 0
        for (const key of files) {
            const readStream = fs.createReadStream(`./${key}.csv`);
            readStream.pipe(
                uploadFromStream
                (
                    'ticketlake-dev',
                    "public-read",
                    key + '.csv',
                    function (err, data) {
                        console.log(data)
                        filesUploaded++
                        if (err) {
                            reject(errorHandler(err, 500, "Error occured uploading file to s3"))
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
        }
    })
}

const convertAndUploadCsv = async (fileLink) => {
    try {
        const result = await promisifyWriteStream(
            request(fileLink),
            fs.createWriteStream('input.csv')
        )

        if (!result) {
            throw errorHandler("Internal server", 500, "Internal server error")
        }

        const readStream = fs.createReadStream('./input.csv');

        const fileReadStream = csv.fromStream(readStream)

        const fileMap = await promisifyReadStream(fileReadStream)

        delete fileMap["undefined"]

        const files = Object.keys(fileMap)

        for (const key of files) {

            const newWriteStream = fs.createWriteStream(`${key}.csv`)

            const csvStream = csv.write(fileMap[key], {headers: true})

            await
                promisifyWriteStream(csvStream, newWriteStream)

        }

        return  await uploadFileToS3(files)

    }
    catch (err) {
        return err
    }
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

