// Load the AWS SDK for Node.js
let constant = require("../constants/constant.js");
let service = require("../service/service.js");
let neritoUtils = require("../utill/neritoUtils.js");

let AWS = require("aws-sdk");
const { v4: uuidv4 } = require("uuid");
const s3 = new AWS.S3();

// Set the region
//const accessKeyId = process.env.accessKeyId
//const secretAccessKey = process.env.secretAccessKey
const region = process.env.region;
const payrolls_table = process.env.payrolls_table;
const config_table = process.env.config_table;
const organization_table = process.env.organization_table;

AWS.config.update({
  //accessKeyId,
  //secretAccessKey,
  region,
});
// Create DynamoDB service object
const documentClient = new AWS.DynamoDB.DocumentClient();
let date = new Date();
let month = date.getMonth() + 1;
let year = date.getFullYear();
let monthYear = month + "-" + year;

module.exports = {
  insertDataIntoDb: async function (data, organization) {
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
        params.RequestItems[payrolls_table] = [];
        let SK = (organization.Id + "#" + monthYear).trim();
        itemData.forEach((item) => {
          //Create unique id for payroll
          let Id = ("PAYROLL#" + uuidv4()).trim();
          params.RequestItems[payrolls_table].push({
            PutRequest: {
              Item: {
                Id: Id,
                SK: SK,
                CompanyId: organization.Id,
                Operation: "04",
                UserName: item["userName"],
                OriginAccount: organization.OriginAccount,
                Email: item["beneficiaryEmail"],
                DestinationAccount: neritoUtils.zeroAppenderOnLeft(
                  item["destinationAccount"],
                  constant.maxLength.DESTINATIONACCOUNT
                ),
                RFC: organization.RFC,
                ImportAmount: neritoUtils.addDecimalPlaces(
                  item["importAmount"]
                ),
                ReferenceDate: neritoUtils.zeroAppenderOnLeft(
                  item["reference"],
                  constant.maxLength.REFERENCE
                ),
                Description: item["description"],
                OriginCurrency: 1,
                DestinationCurrency: 1,
                IVA: "00000000000000",
                ApplicationDate: neritoUtils.formatDate(
                  neritoUtils.dateconverter(item["applicationDate"])
                ),
                PaymentInstructions: item["paymentInstructions"],
                Status: true,
                Month: month,
                Year: date.getFullYear(),
                DateModified: neritoUtils.formatDate(date),
                State: constant.freezeState.PENDING,
              },
            },
          });
        });
        // Push to DynamoDB in batches
        batchCount++;
        const result = await documentClient.batchWrite(params).promise();
        return result;
      })
    )
      .then((values) => {
        return values;
      })
      .catch((error) => {
        console.error("Error: ", error);
        throw new Error(error);
      });
  },
  getPayrollDataByMonthAndYear: async function (orgId) {
    let Id = (orgId + "#" + monthYear).trim();
    const params = {
      TableName: payrolls_table,
      IndexName: "list-payroll-index",
      KeyConditionExpression: "#SK = :SK",
      ExpressionAttributeNames: {
        "#SK": "SK",
      },
      ExpressionAttributeValues: {
        ":SK": Id,
      },
    };
    return await service.getAllData(params);
  },
  getPayrollCurrentMonthIdAndSK: async function (orgId) {
    let Id = (orgId + "#" + monthYear).trim();
    const params = {
      TableName: payrolls_table,
      IndexName: "list-payroll-index",
      KeyConditionExpression: "#SK = :SK",
      FilterExpression: "#State = :StateError or #State = :StatePending",

      ProjectionExpression: "Id, SK",

      ExpressionAttributeNames: {
        "#SK": "SK",
        "#State": "State",
      },
      ExpressionAttributeValues: {
        ":SK": Id,
        ":StateError": 2,
        ":StatePending": 0,
      },
    };
    return await service.query(params);
  },
  getPayrollFreezeData: async function (orgId) {
    let Id = (orgId + "#" + monthYear).trim();

    const params = {
      TableName: payrolls_table,
      IndexName: "list-payroll-index",
      KeyConditionExpression: "#SK = :SK",
      FilterExpression: "#Status=:Status and #State = :StatePending",
      ExpressionAttributeNames: {
        "#SK": "SK",
        "#Status": "Status",
        "#State": "State",
      },
      ExpressionAttributeValues: {
        ":SK": Id,
        ":Status": true,
        ":StatePending": 0,
      },
    };
    return await service.query(params);
  },
  getConfigByType: async function (Type) {
    const params = {
      TableName: config_table,
      KeyConditionExpression: "#Type = :Type",
      ExpressionAttributeNames: {
        "#Type": "Type",
      },
      ExpressionAttributeValues: {
        ":Type": Type,
      },
    };
    return await service.query(params).Items[0].Config;
  },
  updatePayroll: async function (Id, SK, element) {
    try {
      let freezeState;
      let confirmationMessage;
      let operation = element.substring(0, 2).trim();
      let userName = element.substring(2, 15).trim();
      let originAccount = element.substring(15, 35).trim();
      let destinationAccount = element.substring(35, 55).trim();
      let importAmount = element.substring(55, 69).trim();
      let reference = element.substring(69, 79).trim();
      let description = element.substring(79, 109).trim();
      let executionDate = element.substring(109, 117).trim();
      if (!neritoUtils.isEmpty(executionDate)) {
        executionDate = neritoUtils.StringDateConverter(executionDate);
      }
      let confirmation = element.substring(117, 177).trim();
      let applicationDate = element.substring(177, 185).trim();
      if (!neritoUtils.isEmpty(applicationDate)) {
        applicationDate = neritoUtils.StringDateConverter(applicationDate);
      }
      let trackingId = element.substring(185, 215).trim();
      let movementNumber = element.substring(215, 224).trim();
      let accountHolder = element.substring(224, 284).trim();
      if (!neritoUtils.isEmpty(confirmation)) {
        let confirmationCode = confirmation.substring(0, 2).trim();
        confirmationMessage = confirmation.substring(2, 60).trim();
        if (confirmationCode.localeCompare("00") == 0) {
          freezeState = constant.freezeState.SUCCESS;
        } else {
          freezeState = constant.freezeState.Error;
        }
      } else {
        console.error("Response confirmation details not found for ID: ", Id);
        throw "Response confirmation details not found";
      }

      let params = {
        TableName: payrolls_table,
        Key: {
          Id: Id,
          SK: SK,
        },
        UpdateExpression:
          "set #AccountHolder=:AccountHolder,#ApplicationDate=:ApplicationDate,#ExecutionDate=:ExecutionDate,#MovementNumber=:MovementNumber,#ResponseMessage=:ResponseMessage,#State=:State,#TrackingID=:TrackingID",
        ExpressionAttributeNames: {
          "#AccountHolder": "AccountHolder",
          "#ApplicationDate": "ApplicationDate",
          "#ExecutionDate": "ExecutionDate",
          "#MovementNumber": "MovementNumber",
          "#ResponseMessage": "ResponseMessage",
          "#State": "State",
          "#TrackingID": "TrackingID",
        },
        ExpressionAttributeValues: {
          ":State": freezeState,
          ":ResponseMessage": confirmationMessage,
          ":AccountHolder": accountHolder,
          ":ApplicationDate": applicationDate,
          ":ExecutionDate": executionDate,
          ":MovementNumber": movementNumber,
          ":TrackingID": trackingId,
        },
        ReturnValues: "UPDATED_NEW",
      };
      return await service.update(params);
    } catch (error) {
      console.error(
        "Something went wrong while updating payroll bank response for Id: ",
        Id,
        error
      );
      throw "Something went wrong";
    }
  },
  deletePayrolls: async function (data) {
    return deletePayrolls(data);
  },
  updatePayrollFileDetails: async function (payrollFileName) {
    let params = {
      TableName: organization_table,
      Key: {
        Id: "NERITO#af427acc-b8f7-4455-ab4b-4f61042896f4",
        SK: "METADATA#af427acc-b8f7-4455-ab4b-4f61042896f4",
      },
      UpdateExpression: "set PayrollFile = :PayrollFile",
      ExpressionAttributeValues: {
        ":PayrollFile": payrollFileName,
      },
      ReturnValues: "UPDATED_NEW",
    };
    return service.update(params);
  },
};

async function deletePayrolls(data) {
  const batches = [];
  const BATCH_SIZE = 25;
  data = data.Items;
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
      params.RequestItems[payrolls_table] = [];
      itemData.forEach((item) => {
        //Create param to save employee in batches into dynamoDB
        params.RequestItems[payrolls_table].push({
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
}
