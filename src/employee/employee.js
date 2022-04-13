let service = require("../service/service.js");
let employeeService = require("../service/employeeService.js");

let neritoUtils = require("../utill/neritoUtils.js");
let constant = require("../constants/constant.js");
let csvValidator = require("./employeeCsvValidator.js");

async function insertEmployee(orgId, fileId) {
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
      const csvValidationData = await csvValidator.validateCsv(csvFile, orgId);
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
      const result = await employeeService.getEmpCurrentMonthIdAndSK(orgId);
      if (!neritoUtils.isEmpty(result)) {
        let empJson = JSON.parse(JSON.stringify(result));
        if (!neritoUtils.isEmpty(empJson)) {
          const result = await employeeService.deleteEmployees(empJson);
        }
      }
    } catch (err) {
      console.error(
        "Failed to delete old payroll data of this month by orgId : " + orgId,
        err
      );
      throw "Something went wrong";
    }

    try {
      const result = await employeeService.insertDataIntoDb(csvJson, orgId);
      if (!neritoUtils.isEmpty(result)) {
        let csvJson = JSON.parse(JSON.stringify(result));
        for (let i = 0; i < csvJson.length; i++) {
          var row = csvJson[i];
          if (
            !neritoUtils.isEmpty(row) &&
            !row.hasOwnProperty("UnprocessedItems")
          ) {
            isAllInserted = false;
          }
        }
      } else {
        isAllInserted = false;
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
      const result = await employeeService.getEmpDataByMonthAndYear(orgId);
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
module.exports = insertEmployee;
