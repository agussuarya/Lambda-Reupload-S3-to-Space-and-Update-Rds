const AWS = require('aws-sdk');
const s3 = new AWS.S3();
const mysql = require('mysql');

exports.handler = async function(event, context, callback) {
    // Object key may have spaces or unicode non-ASCII characters.
    let srcKey = decodeURIComponent(event.Records[0].s3.object.key.replace(/\+/g, " "));
    let srcBucket = event.Records[0].s3.bucket.name;
    let dstSpaceBucket = process.env.SPACE_BUCKET;
    let dstKey = srcKey;

    // Validate that source and destination are different buckets.
    isSameBucket(srcBucket, dstSpaceBucket);

    // Read the image from S3
    let srcStream = await readFileFromS3(srcBucket, srcKey);
    if (srcStream.status === false) {
        return srcStream;
    }
    srcStream = srcStream.data;

    // Upload to space
    let uploadResult = await uploadFileToSpace(dstSpaceBucket, dstKey, srcStream.Body);
    if (uploadResult.status === false) {
        return uploadResult;
    }

    // Get full path space digital ocean
    const fullPathSpace = getFullPatchFileSpace(process.env.SPACE_REGION, dstSpaceBucket, dstKey);

    // Update row table in rds
    const fileName = getFilename(srcKey);
    const id  = getMetadataIdFromFilename(fileName);
    return await updateDataTableRds(id, fullPathSpace);
};

/**
 * Check if source bucket & destination bucket same or not
 * @param srcBucket
 * @param destBucket
 * @returns {{message: string, status: boolean}}
 */
function isSameBucket(srcBucket, destBucket) {
    if (srcBucket === destBucket) {
        return {
            'status': false,
            'message': 'Source and destination buckets are the same.'
        };
    }
}

/**
 * Relative path = sub_folder/filename.mp4 => filename.mp4
 * @param relativePath
 * @returns {string}
 */
function getFilename(relativePath) {
    return relativePath.split('/').pop();
}

/**
 * Custom with your logic...
 *
 * Example:
 * Filename = xxx_60.mp4 => id = 60
 *
 * @param filename
 */
function getMetadataIdFromFilename(filename) {
    // Get without extension
    let filenames = filename.split('.');
    filenames.pop();

    // Get metadata id
    return filenames[0].split('_').pop();
}

/**
 * Read file from s3 bucket
 * @param bucket
 * @param key
 * @returns {Promise<*>}
 */
async function readFileFromS3(bucket, key) {
    let paramsSrc = {
        Bucket: bucket,
        Key: key
    };
    try {
        const srcStream = await s3.getObject(paramsSrc).promise();
        return {
            'status': true,
            'data': srcStream,
            'message': 'Success read s3 object.'
        };
    } catch(err) {
        return {
            'status': false,
            'message': 'Failed read s3 object.'
        };
    }
}

/**
 * Upload file to space
 * @param bucket
 * @param key
 * @param streamData
 * @returns {Promise<*>}
 */
async function uploadFileToSpace(bucket, key, streamData) {
    // Set S3 endpoint to DigitalOcean Spaces
    const accessKeyId = process.env.SPACE_ACCESS_KEY;
    const secretAccessKey = process.env.SPACE_SECRET_KEY;
    const region = 'sgp1';
    const spacesEndpoint = new AWS.Endpoint(region + '.digitaloceanspaces.com');
    console.log(spacesEndpoint.href);
    const s3New = new AWS.S3({
        endpoint: spacesEndpoint,
        accessKeyId: accessKeyId,
        secretAccessKey: secretAccessKey
    });

    // Re upload file to space
    let paramsDst = {
        Bucket: bucket,
        Key: key,
        Body: streamData,
        ACL: 'public-read',
    };
    try {
        const resultUpload = await s3New.putObject(paramsDst).promise();
        return {
            'status': true,
            'message': 'Success upload to space.'
        };
    } catch(err) {
        return {
            'status': false,
            'message': 'Failed upload to space.'
        };
    }
}

/**
 * Get full path file from space
 * @param region
 * @param bucket
 * @param key
 * @returns {string}
 */
function getFullPatchFileSpace(region, bucket, key) {
    return 'https://' + bucket + '.' + region + '.digitaloceanspaces.com/' + key;
}

/**
 * Custom with your logic...
 *
 * Example: we will run query sql (Update table).
 * @param id
 * @param videoUrl
 * @returns {Promise<*>}
 */
async function updateDataTableRds(id, videoUrl) {
    const tableToUpdate = process.env.RDS_UPDATE_TABLE_NAME;
    const additionalUpdate = process.env.RDS_UPDATE_ADDITIONAL_UPDATE;
    const pkName = process.env.RDS_UPDATE_PK_NAME;
    const querySql = "UPDATE " + tableToUpdate + " SET video_url='" + videoUrl + "', " + additionalUpdate + " WHERE " + pkName + "=" + id + ";";
    let connection = mysql.createConnection({
        host: process.env.RDS_HOST,
        user: process.env.RDS_USER,
        password: process.env.RDS_PASSWORD,
        database: process.env.RDS_DATABASE,
    });

    let promise = new Promise(function(resolve, reject) {
        connection.query(
            querySql,
            function (error, results, fields) {
            if (error) {
                connection.destroy();
                reject(error)
            } else {
                connection.end(function (error) { reject(error);});
                resolve(results);
            }
        });
    });
    try {
        await promise;
        return {
            'status': true,
            'message': 'Success update table.'
        };
    } catch(err) {
        return {
            'status': false,
            'message': 'Failed update table.',
            'error': err
        };
    }
}