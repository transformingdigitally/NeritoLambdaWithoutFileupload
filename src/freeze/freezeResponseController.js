let service = require("../service/service.js");
let neritoUtils = require("../utill/neritoUtils.js");
const csvjson = require("csvjson");
let constant = require("../constants/constant.js");
let employeeService = require("../service/employeeService.js");
let payrollService = require("../service/payrollService.js");

const freeze_temp_bucket = process.env.freeze_temp_bucket;
const payrollDisbursementFilesBucket =
  process.env.payrollDisbursementFilesBucket;
const payrollDisbursementFilesBucketRegion =
  process.env.payrollDisbursementFilesBucketRegion;
const payrollNeritoBucketRegion = process.env.payrollNeritoBucketRegion;
let sentFileMap;
let organizationsList = [];

module.exports = {
  freezeResponse: async function (json) {
    try {
      let orgList = await service.getOrganizationsList();
      orgList = orgList.Items;
      orgList.forEach((element) => {
        organizationsList.push(element.Id);
      });
      const error = await requestvalidator(json);
      console.log("error", error);
      if (!neritoUtils.isEmpty(error)) {
        return error;
      }

      for (const element of json) {
        try {
          if (
            element.type.localeCompare(constant.freezeBucket.PAYROLL_OUTPUT) ==
            0
          ) {
            await readDataFromS3(element, constant.freezeBucket.PAYROLL_OUTPUT);
          } else if (
            element.type.localeCompare(constant.freezeBucket.REGISTER_PAYROLL) ==
            0
          ) {
            await readDataFromS3(element, constant.freezeBucket.REGISTER_PAYROLL);
          }
        } catch (err) {
          throw "Something went wrong";
        }
      }
      let response = {
        status: "Successfully updated",
      };
      return neritoUtils.successResponseJson(response, 200);
    } catch (err) {
      console.error("Failed to upload data on S3 : ", err);
      throw "Something went wrong";
    }
  },
};

async function updateFreezeResponse(data, type) {
  try {
    // Split rows by new line
    let csvData = data.split("\n");
    for (let i = 0; i < csvData.length; i++) {
      let element = csvData[i];
      let rowNumber = i + 1;
      if (!neritoUtils.isEmpty(sentFileMap.get(rowNumber))) {
        let Id = sentFileMap.get(rowNumber).split("|")[0];
        let SK = sentFileMap.get(rowNumber).split("|")[1];
        if (type.localeCompare(constant.freezeBucket.PAYROLL_OUTPUT) == 0) {
          const result = await payrollService.updatePayroll(Id, SK, element);
        } else if (
          type.localeCompare(constant.freezeBucket.REGISTER_PAYROLL) == 0
        ) {
          const result = await employeeService.updateEmplyee(Id, SK, element);
        }
      }
    }
  } catch (error) {
    console.error(
      "Something went wrong while updating data into Db",
      JSON.stringify(error, null, 2)
    );
    throw "Something went wrong";
  }
  return true;
}

function csvToJson(data, delimiter) {
  let csvData;
  try {
    var options = {
      delimiter: delimiter, // optional
      quote: '"', // optional
    };
    csvData = csvjson.toObject(data, options);
  } catch (error) {
    console.error(
      "Something went wrong while adding data into Db",
      JSON.stringify(error, null, 2)
    );
    throw "Something went wrong";
  }
  return csvData;
}

async function requestvalidator(json) {
  try {
    if (neritoUtils.isEmpty(json)) {
      return neritoUtils.errorResponseJson("Body json can not be empty", 400);
    }
    let count = 1;
    for (const element of json) {
      if (neritoUtils.isEmpty(element.type)) {
        return neritoUtils.errorResponseJson(
          "Type can not be empty at position " + count,
          400
        );
      } else if (
        element.type != constant.freezeBucket.REGISTER_PAYROLL &&
        element.type != constant.freezeBucket.PAYROLL_OUTPUT
      ) {
        return neritoUtils.errorResponseJson(
          "Invalid type at position " + count,
          400
        );
      }
      if (neritoUtils.isEmpty(element.orgId)) {
        return neritoUtils.errorResponseJson(
          "orgId can not be empty at position " + count,
          400
        );
      } else if (!organizationsList.includes(element.orgId)) {
        return neritoUtils.errorResponseJson(
          "Incorrect orgId at position " + count,
          400
        );
      }
      if (neritoUtils.isEmpty(element.filename)) {
        return neritoUtils.errorResponseJson(
          "filename can not be empty at position " + count,
          400
        );
      }
      count++;
    }
  } catch (error) {
    console.error("Something went wrong while parsing request", error);
    throw "Something went wrong";
  }
}

async function readDataFromS3(json, type) {
  let count = 1;
  let sentFilename;
  sentFileMap = new Map();
  try {
    // Get filename by splitting success filename (as we need file name like AC-1645994295424.txt and we are getting EXITO-AC-1645994295424.txt)
    sentFilename = json.filename.split(".")[0];
    sentFilename = sentFilename + ".txt";

    // Create folder name by bucket, orgId and type i.e. payroll-nerito-us-west-1/AC/IN/ORG#20624f7d-c698-4eb2-94f3-7de31468161f/
    // temp folder to maintain row number
    const tempFreezeBucket =
      freeze_temp_bucket + "/" + type + "/IN/" + json.orgId;

    // Get txt data from temp folder (as we need to remember row number which we have sent to bank)
    const sentCsvFile = await service.readFreezeResponseFilesFromS3(
      sentFilename,
      tempFreezeBucket,
      payrollNeritoBucketRegion
    );
    const sentFileJsonArray = csvToJson(sentCsvFile.Body.toString(), ",");

    // Create a map of txt data with row number
    sentFileJsonArray.forEach((element) => {
      sentFileMap.set(count, element.Id + "|" + element.SK);
      count++;
    });

    // Create folder name by bucket, orgId and type i.e. payroll-disbursement-files-bucket-dev-us-west-2/AC/OUT/ORG#20624f7d-c698-4eb2-94f3-7de31468161f/
    // Getting bank response in OUT folder
    const freezeBucket =
      payrollDisbursementFilesBucket + "/" + type + "/OUT/" + json.orgId;

    // Get txt data from OUT folder
    const responseCsvFile = await service.readFreezeResponseFilesFromS3(
      json.filename,
      freezeBucket,
      payrollDisbursementFilesBucketRegion
    );
    const result = await updateFreezeResponse(
      responseCsvFile.Body.toString(),
      type
    );
  } catch (error) {
    console.error(
      "Something went wrong while fetching data from temp folder",
      error
    );
    throw "Something went wrong";
  }
}
