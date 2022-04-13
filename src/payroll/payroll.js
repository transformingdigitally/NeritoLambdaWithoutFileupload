let service = require("../service/service.js");
let payrollService = require("../service/payrollService.js");

let neritoUtils = require("../utill/neritoUtils.js");
let csvValidator = require("./payrollCsvValidator.js");
let constant = require("../constants/constant.js");

async function insertPayroll(orgId, fileId) {
  try {
    let fileName;
    let isAllInserted = true;
    let csvJson;
    let csvFile;

    try {
      let fileDetails = await service.getDatabyIdAndSK(orgId, fileId);
      if (neritoUtils.isEmpty(fileDetails)) {
        console.error("CSV file details not found by fileId: " + fileId);
        throw "Something went wrong";
      }
      fileDetails = JSON.parse(JSON.stringify(fileDetails));
      fileDetails = fileDetails.Items[0];
      if (
        fileDetails.CsvStatus.localeCompare(constant.csvStatus.PENDING) == 0
      ) {
        fileName = fileDetails.CsvName;
      } else {
        console.error("No CSV file found Pending By fileId: " + fileId);
        throw "Something went wrong";
      }
    } catch (err) {
      console.error("CSV file details not found by fileId: " + fileId, err);
      throw "Something went wrong";
    }
    try {
      let isFileExist = await service.isFileExist(fileName);
      if (!isFileExist) {
        console.error("CSV file not found with this Name: " + fileName);
        return neritoUtils.errorResponseJson("NotFound", 400);
      }
    } catch (err) {
      console.error("CSV file not found with this Name: " + fileName, err);
      throw "Something went wrong";
    }
    try {
      csvFile = await service.readCsvFromS3(fileName);
      if (neritoUtils.isEmpty(csvFile)) {
        console.error("Empty CSV File Found : " + fileName);
        throw "Something went wrong";
      }
    } catch (err) {
      console.error("Failed to Parse CSV File : " + fileName, err);
      throw "Something went wrong";
    }
    try {
      const csvValidationData = await csvValidator.validateCsv(csvFile);
      if (
        neritoUtils.isEmpty(csvValidationData) ||
        neritoUtils.isEmpty(csvValidationData.data)
      ) {
        console.error("csvValidationData Error : " + fileName);
        throw "Something went wrong";
      }
      if (!neritoUtils.isEmpty(csvValidationData.inValidMessages)) {
        console.error(
          "csvValidationData Error : " + fileName,
          csvValidationData
        );
        return neritoUtils.errorResponseJson(csvValidationData, 400);
      }
      if (csvValidationData.data.length < 1) {
        console.error("Row count is less than 1 : " + fileName);
        return neritoUtils.errorResponseJson("Row count is less than 10", 400);
      }
      csvJson = csvValidationData.data;
    } catch (err) {
      console.error("Failed to Parse CSV File : " + fileName, err);
      throw "Something went wrong";
    }
    try {
      const result = await payrollService.getPayrollCurrentMonthIdAndSK(orgId);
      if (!neritoUtils.isEmpty(result)) {
        let payrollJson = JSON.parse(JSON.stringify(result));
        if (!neritoUtils.isEmpty(payrollJson)) {
          const result = await payrollService.deletePayrolls(payrollJson);
        }
      }
    } catch (err) {
      console.error(
        "Failed to delete old payroll data of this month by orgId : " + orgId
      );
      throw "Something went wrong";
    }

    try {
      let organization = await service.getOrgDataById(orgId);
      if (
        neritoUtils.isEmpty(organization) ||
        neritoUtils.isEmpty(organization.Items[0])
      ) {
        console.error("Failed to fetch organization data by orgId : " + orgId);
        throw "Something went wrong";
      }
      organization = organization.Items[0];
      const result = await payrollService.insertDataIntoDb(
        csvJson,
        organization
      );
      if (!neritoUtils.isEmpty(result)) {
        let csvJson = JSON.parse(JSON.stringify(result));
        for (let i = 0; i < csvJson.length; i++) {
          var row = csvJson[i];
          if (!row.hasOwnProperty("UnprocessedItems")) {
            isAllInserted = false;
          }
        }
      }
      if (!isAllInserted) {
        console.error("Failed to Insert Data in Db: " + fileName);
        throw "Something went wrong";
      }
    } catch (err) {
      console.error("Failed to insert payroll data into db : " + fileName, err);
      throw "Something went wrong";
    }

    try {
      const result = await service.updateCsvDetails(orgId, fileId);
    } catch (err) {
      console.error("Failed to update Csv data : ", err);
      throw "Something went wrong";
    }

    try {
      const result = await payrollService.getPayrollDataByMonthAndYear(orgId);
      return neritoUtils.successResponseJson(result, 200);
    } catch (err) {
      console.error("Failed to fetch data from db : ", err);
      throw "Something went wrong";
    }
  } catch (err) {
    console.error("Something went wrong", err);
    return neritoUtils.errorResponseJson(err, 500);
  }
}
module.exports = insertPayroll;
