// Load the AWS SDK for Node.js
let constant = require("../constants/constant.js");
let neritoUtils = require("../utill/neritoUtils.js");

// Set the region
//const accessKeyId = process.env.accessKeyId
//const secretAccessKey = process.env.secretAccessKey
const region = process.env.region;
const base_bucket_name = process.env.bucket_name;
const freeze_temp_bucket = process.env.freeze_temp_bucket;
const payrollDisbursementFilesBucket =
  process.env.payrollDisbursementFilesBucket;
const organization_table = process.env.organization_table;

let AWS = require("aws-sdk");
const { v4: uuidv4 } = require("uuid");
const bucketRegion = process.env.bucketRegion;
const s3 = new AWS.S3();

AWS.config.update({
  //accessKeyId,
  //secretAccessKey,
  region,
});
// Create DynamoDB service object
const documentClient = new AWS.DynamoDB.DocumentClient();

module.exports = {
  isFileExist: async function (fileName) {
    const params = {
      Bucket: base_bucket_name + "/temp",
      Key: fileName,
    };
    const exists = await s3
      .headObject(params)
      .promise()
      .then(
        () => true,
        (err) => {
          if (err.code === "NotFound") {
            return false;
          }
          throw err;
        }
      );
    return exists;
  },

  readCsvFromS3: async function (fileName) {
    const params = {
      Bucket: base_bucket_name + "/temp",
      Key: fileName,
    };
    try {
      const data = await s3.getObject(params).promise();
      return data;
    } catch (err) {
      console.error("Failed to get file from S3", err);
      throw new Error(err);
    }
  },
  readFreezeResponseFilesFromS3: async function (fileName, bucket, region) {
    const s3 = new AWS.S3({ region: region });

    const params = {
      Bucket: bucket,
      Key: fileName,
    };
    try {
      const data = await s3.getObject(params).promise();
      return data;
    } catch (err) {
      console.error("Failed to get file from S3", err);
      throw new Error(err);
    }
  },
  putObjectOnS3: async function (fullFileName, fileContent, bucket, region) {
    const s3 = new AWS.S3({ region: region });

    let isUploaded = false;
    // Upload the file to S3
    var bucketParams = {};
    bucketParams.Bucket = bucket;
    bucketParams.Key = fullFileName;
    bucketParams.Body = fileContent;
    try {
      let putObjectPromise = await s3
        .putObject(bucketParams, function (err, data) {
          if (err) {
            console.error(
              "unable to upload file:" + fullFileName + "  ",
              JSON.stringify(err, null, 2)
            );
            return isUploaded;
          }
          if (data) {
            isUploaded = true;
            return isUploaded;
          }
        })
        .promise();
    } catch (err) {
      console.error("Something went wrong while uploading object on s3", err);
      isUploaded = false;
      return isUploaded;
    }
    return isUploaded;
  },
  getDatabyIdAndSK: async function (Id, SK) {
    const params = {
      TableName: organization_table,
      KeyConditionExpression: "#Id = :Id and #SK= :SK",
      ExpressionAttributeNames: {
        "#Id": "Id",
        "#SK": "SK",
      },
      ExpressionAttributeValues: {
        ":Id": Id,
        ":SK": SK,
      },
    };
    return query(params);
  },
  getOrganizationsList: async function () {
    const params = {
      TableName: organization_table,
      IndexName: "Type-SK-index",
      KeyConditionExpression: "#Type = :Type",
      ExpressionAttributeNames: {
        "#Type": "Type",
      },
      ExpressionAttributeValues: {
        ":Type": "METADATA",
      },
    };
    return query(params);
  },
  deleteByIdAndSk: async function (Id, SK) {
    const params = {
      TableName: organization_table,
      Key: {
        Id: Id,
        SK: SK,
      },
    };
    let result = await documentClient
      .delete(params)
      .promise()
      .catch((error) => {
        console.error("Error: ", error);
        throw new Error(error);
      });
    return result;
  },
  updateCsvDetails: async function (orgId, fileId) {
    let params = {
      TableName: organization_table,
      Key: {
        Id: orgId,
        SK: fileId,
      },
      UpdateExpression: "set CsvStatus = :CsvStatus",
      ExpressionAttributeValues: {
        ":CsvStatus": constant.csvStatus.COMPLETED,
      },
      ReturnValues: "UPDATED_NEW",
    };
    return update(params);
  },

  update: async function (params) {
    return update(params);
  },
  getOrgDataById: async function (orgId) {
    const params = {
      TableName: organization_table,
      KeyConditionExpression: "#Id = :Id and begins_with(#SK, :SK)",
      ExpressionAttributeNames: {
        "#Id": "Id",
        "#SK": "SK",
      },
      ExpressionAttributeValues: {
        ":Id": orgId,
        ":SK": "METADATA#",
      },
    };
    return await query(params);
  },
  query: async function (params) {
    return query(params);
  },
  delete: async function (data, table_name) {
    const batches = [];
    const BATCH_SIZE = 25;

    while (data.length > 0) {
      batches.push(data.splice(0, BATCH_SIZE));
    }
    let batchCount = 0;
    // Save each batch
    return await Promise.all(
      batches.map(async (itemData) => {
        // Set up the params object for the DDB call
        const params = {
          RequestItems: {},
        };
        params.RequestItems[table_name] = [];
        itemData.forEach((item) => {
          //Create param to save employee in batches into dynamoDB
          params.RequestItems[table_name].push({
            DeleteRequest: {
              Key: {
                Id: item.Id,
                SK: item.SK,
              },
            },
          });
        });
        // Push to DynamoDB in batches
        batchCount++;
        const result = await documentClient.batchWrite(params).promise();
        while (!neritoUtils.isEmpty(result.UnprocessedItems)) {
          const paramsUnprocessedItems = {
            RequestItems: result.UnprocessedItems,
          };
          try {
            result = await documentClient.batchWrite(paramsUnprocessedItems);
          } catch (error) {
            console.error("Error", error);
          }
        }
        return result;
      })
    )
      .then(async function (values) {
        return values;
      })
      .catch((error) => {
        console.error("Error: ", error);
        throw new Error(error);
      });
  },
  getAllData: async function (params) {
    return getAllData(params);
  },
};
async function query(params) {
  let result = await documentClient
    .query(params)
    .promise()
    .catch((error) => {
      console.error("Error: ", error);
      throw new Error(error);
    });
  return result;
}

const getAllData = async (params) => {
  const _getAllData = async (params, startKey) => {
    if (startKey) {
      params.ExclusiveStartKey = startKey;
    }
    let result = await documentClient.query(params).promise();
    return result;
  };
  let lastEvaluatedKey = null;
  let rows = [];
  do {
    const result = await _getAllData(params, lastEvaluatedKey);
    rows = rows.concat(result.Items);
    lastEvaluatedKey = result.LastEvaluatedKey;
  } while (lastEvaluatedKey);
  return rows;
};

async function update(params) {
  let dbData;
  try {
    dbData = await documentClient
      .update(params, function (err, data) {
        if (err) {
          console.error(
            "Unable to add item. Error JSON:",
            JSON.stringify(err, null, 2)
          );
          return false;
        }
      })
      .promise();
  } catch (error) {
    console.error(
      "Something went wrong while adding data into Db",
      JSON.stringify(error, null, 2)
    );
    return false;
  }
  return dbData;
}
