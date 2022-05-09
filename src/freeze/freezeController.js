let service = require("../service/service.js");
let neritoUtils = require("../utill/neritoUtils.js");
const csvjson = require("csvjson");
let constant = require("../constants/constant.js");
let employeeService = require("../service/employeeService.js");
let payrollService = require("../service/payrollService.js");

const utf8 = require("utf8");

const freeze_temp_bucket = process.env.freeze_temp_bucket;
const payrollDisbursementFilesBucket =
  process.env.payrollDisbursementFilesBucket;
const payrollDisbursementFilesBucketRegion =
  process.env.payrollDisbursementFilesBucketRegion;
const payrollNeritoBucketRegion = process.env.payrollNeritoBucketRegion;

async function freezeData(orgId, action) {
  let csvJson,
    options,
    freezeBucket,
    freezeTempBucket,
    result,
    fullFileName,
    orgDetails,
    strCount;
  let jsonArray = [];
  try {
    let date = new Date();

    // get Organization data
    let organization = await service.getOrgDataById(orgId);
    if (
      !neritoUtils.isEmpty(organization) &&
      !neritoUtils.isEmpty(organization.Items[0])
    ) {
      organization = organization.Items[0];
    }

    // get Nerito config data for updating global counter for file.
    try {
      let orgList = await service.getDatabyIdAndSK(
        "NERITO#af427acc-b8f7-4455-ab4b-4f61042896f4",
        "METADATA#af427acc-b8f7-4455-ab4b-4f61042896f4"
      );
      orgDetails = orgList.Items[0];
    } catch (err) {
      console.error("CSV file details not found by fileId: ", err);
      throw "Something went wrong";
    }

    // Option for JsonTOcsv we have to remove headers from csv file
    options = {
      headers: "none",
      delimiter: ",",
    };

    if (action.localeCompare(constant.action.FREEZE) == 0) {
      // For freezing AC records.
      let strDate;
      if (!neritoUtils.isEmpty(orgDetails)) {
        let accountFile = orgDetails.AccountFile;
        if (!neritoUtils.isEmpty(accountFile)) {
          accountFile = accountFile.substring(8, 17);
          strCount = accountFile.substring(6, 9);
          strDate = accountFile.substring(0, 6);
        }
      }

      // Bucket name with sub folder to freeze AC record
      freezeBucket =
        payrollDisbursementFilesBucket +
        constant.separator +
        constant.freezeBucket.REGISTER_PAYROLL +
        constant.separator +
        constant.folder.IN +
        constant.separator +
        orgId;

      // Temp bucket name with sub for managing same row number
      freezeTempBucket =
        freeze_temp_bucket +
        constant.separator +
        constant.freezeBucket.REGISTER_PAYROLL +
        constant.separator +
        constant.folder.IN +
        constant.separator +
        orgId;

      // Get AC data to freeze (only failed and pending records will be send to bank)
      result = await employeeService.getEmpFreezeData(orgId);

      // Intialize file counter with 001
      let count = "001";

      // Change counter if previouse file created on the same
      if (
        !neritoUtils.isEmpty(strDate) &&
        strDate.localeCompare(neritoUtils.dateFormatter(date)) == 0
      ) {
        strCount = parseInt(strCount);
        strCount++;
        count = strCount.toString().padStart(3, "0");
      }
      // File name: length of 21 positions, where RP(2) + NumEmp(6) + AAMMDD(6) + Sequence(3) + .txt(4)
      // For example Example: RP0391235160322001.txt
      // Emp number will be hard coded 391235.
      fullFileName =
        constant.freezeBucket.REGISTER_PAYROLL +
        constant.empNumber +
        neritoUtils.dateFormatter(date) +
        count +
        constant.fileFormat.TXT;

      if (!neritoUtils.isEmpty(result)) {
        csvJson = JSON.parse(JSON.stringify(result));
        csvJson = csvJson.Items;
        if (!neritoUtils.isEmpty(csvJson)) {
          // Arrange all AC records by custom order for csv creation i.e.(CompanyId, OperationType .. etc)
          csvJson.forEach((element) => {
            let json = {};
            json.A_CompanyId = neritoUtils.spacesAppenderOnRight(
              element.CompanyId,
              constant.maxLength.COMPANYID
            );
            json.B_OperationType = neritoUtils.spacesAppenderOnRight(
              element.OperationType,
              constant.maxLength.OPERATIONTYPE
            );
            json.C_Name = neritoUtils.spacesAppenderOnRight(
              "",
              constant.maxLength.NAME
            );
            json.D_RFC = neritoUtils.spacesAppenderOnRight(
              "",
              constant.maxLength.RFC
            );
            json.E_Phonenumber = neritoUtils.spacesAppenderOnRight(
              "",
              constant.maxLength.PHONENUMBERFREEZE
            );
            json.F_Contact = neritoUtils.spacesAppenderOnRight(
              "",
              constant.maxLength.CONTACT
            );
            json.G_Email = neritoUtils.spacesAppenderOnRight(
              "",
              constant.maxLength.EMAIL
            );
            json.H_AccountType = neritoUtils.spacesAppenderOnRight(
              element.AccountType,
              constant.maxLength.TYPEACCOUNT
            );
            json.I_Currency = neritoUtils.spacesAppenderOnRight(
              element.Currency,
              constant.maxLength.CURRENCY
            );
            json.J_BankId = neritoUtils.spacesAppenderOnRight(
              element.BankId,
              constant.maxLength.BANKID
            );
            json.K_Entity = neritoUtils.spacesAppenderOnRight(
              "01",
              constant.maxLength.ENTITY
            );
            json.L_Plaza = neritoUtils.spacesAppenderOnRight(
              "001",
              constant.maxLength.PLAZA
            );
            json.M_AccountClabe = neritoUtils.appendValueOnLeft(
              element.AccountClabe,
              constant.maxLength.ACCOUNTCLABE,
              "0"
            );
            json.N_ExtraInfo = neritoUtils.appendValueOnLeft(
              "",
              constant.maxLength.EXTRAINFO,
              "1"
            );
            jsonArray.push(json);
          });
        } else {
          throw "No records to freeze";
        }
      }
    } else if (action.localeCompare(constant.action.FREEZE_PAYROLL) == 0) {
      // For freezing PP records.
      let strDate;
      if (!neritoUtils.isEmpty(orgDetails)) {
        let payrollFile = orgDetails.PayrollFile;
        if (!neritoUtils.isEmpty(payrollFile)) {
          payrollFile = payrollFile.substring(8, 17);
          strCount = payrollFile.substring(6, 9);
          strDate = payrollFile.substring(0, 6);
        }
      }
      freezeBucket =
        payrollDisbursementFilesBucket +
        constant.separator +
        constant.freezeBucket.PAYROLL_OUTPUT +
        constant.separator +
        constant.folder.IN +
        constant.separator +
        orgId;

      freezeTempBucket =
        freeze_temp_bucket +
        constant.separator +
        constant.freezeBucket.PAYROLL_OUTPUT +
        constant.separator +
        constant.folder.IN +
        constant.separator +
        orgId;

      result = await payrollService.getPayrollFreezeData(orgId);
      let count = "001";
      if (
        !neritoUtils.isEmpty(strDate) &&
        strDate.localeCompare(neritoUtils.dateFormatter(date)) == 0
      ) {
        strCount = parseInt(strCount);
        strCount++;
        count = strCount.toString().padStart(3, "0");
      }

      // File name: length of 21 positions, where PP(2) + numComp(6) + YYMMDD(6) + Consecutive(3) + .TxT(4)
      // For Example: PP035789060303001.TXT
      // numComp will be hard coded 391235.
      fullFileName =
        constant.freezeBucket.PAYROLL_OUTPUT +
        constant.compNumber +
        neritoUtils.dateFormatter(date) +
        count +
        constant.fileFormat.TXT;

      if (!neritoUtils.isEmpty(result)) {
        csvJson = JSON.parse(JSON.stringify(result));
        csvJson = csvJson.Items;
        if (!neritoUtils.isEmpty(csvJson)) {
          // Arrange all PP records by custom order for csv creation i.e.(Operation, Username .. etc)
          csvJson.forEach((element) => {
            let json = {};
            json.A_Operation = neritoUtils.spacesAppenderOnRight(
              element.Operation,
              constant.maxLength.OPERATIONTYPE
            );
            json.B_CompanyId = neritoUtils.spacesAppenderOnRight(
              organization.Id,
              constant.maxLength.COMPANYID
            );
            json.C_OriginAccount = neritoUtils.appendValueOnLeft(
              element.OriginAccount,
              constant.maxLength.ORIGINACCOUNT,
              "0"
            );
            json.D_DestinationAccount = neritoUtils.appendValueOnLeft(
              element.DestinationAccount,
              constant.maxLength.DESTINATIONACCOUNT,
              "0"
            );
            json.E_ImportAmount = neritoUtils.fixDecimalPlaces(
              element.ImportAmount,
              constant.maxLength.IMPORTAMOUNT
            );
            json.F_ReferenceDate = neritoUtils.appendValueOnLeft(
              element.ReferenceDate,
              constant.maxLength.REFERENCE,
              "0"
            );
            json.G_Description = neritoUtils.spacesAppenderOnRight(
              element.Description,
              constant.maxLength.DESCRIPTION
            );
            json.H_OriginCurrency = element.OriginCurrency;
            json.I_DestinationCurrency = element.DestinationCurrency;
            json.J_RFC = neritoUtils.spacesAppenderOnRight(
              organization.RFC,
              constant.maxLength.RFC
            );
            json.K_IVA = neritoUtils.spacesAppenderOnRight(
              element.IVA,
              constant.maxLength.IVA
            );
            json.L_BeneficiaryEmail = neritoUtils.spacesAppenderOnRight(
              element.Email,
              constant.maxLength.EMAIL
            );
            json.M_ApplicationDate = neritoUtils.spacesAppenderOnRight(
              element.ApplicationDate.replace(/-/g, ""),
              constant.maxLength.APPLICATIONDATE
            );
            json.N_PaymentInstructions = neritoUtils.spacesAppenderOnRight(
              element.UserName,
              constant.maxLength.PAYMENTINSTRUCTIONS
            );
            jsonArray.push(json);
          });
        } else {
          throw "No records to freeze";
        }
      }
    }
    if (!neritoUtils.isEmpty(jsonArray)) {
      // Write bucket to temp folder for maintaing the row number to map bank response
      const tempCsvData = csvjson.toCSV(csvJson, { headers: "key" });
      const isTempFileUploaded = await service.putObjectOnS3(
        fullFileName,
        tempCsvData,
        freezeTempBucket,
        payrollNeritoBucketRegion
      );

      let csvData = csvjson.toCSV(jsonArray, options);
      csvData = csvData.replace(/\r?\n/, "");
      csvData = csvData.replace(/,/g, "");
      csvData = utf8.encode(csvData);
      try {
        if (action.localeCompare(constant.action.FREEZE_PAYROLL) == 0) {
          const result = await payrollService.updatePayrollFileDetails(
            fullFileName
          );
        } else if (action.localeCompare(constant.action.FREEZE) == 0) {
          const result = await employeeService.updateAccountFileDetails(
            fullFileName
          );
        }
      } catch (err) {
        console.error("Failed to update payroll file on server : ", err);
        throw "Something went wrong";
      }

      const isFileUploaded = await service.putObjectOnS3(
        fullFileName,
        csvData,
        freezeBucket,
        payrollDisbursementFilesBucketRegion
      );

      if (!isFileUploaded) {
        console.error("Error while uploading file: " + fullFileName);
        throw "Something went wrong";
      }
      let response = {
        orgId: orgId,
        status: "Successfully freezed",
        fileName: fullFileName,
      };

      return neritoUtils.successResponseJson(response, 200);
    }
  } catch (err) {
    console.error("Failed to upload data on S3 : ", err);
    throw "Something went wrong";
  }
}
module.exports = freezeData;
