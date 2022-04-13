// Load the AWS SDK for Node.js
let employee = require("./src/employee/employee.js");
let neritoUtils = require("./src/utill/neritoUtils.js");
let constant = require("./src/constants/constant.js");
let freezeController = require("./src/freeze/freezeController.js");
let freezeResponseController = require("./src/freeze/freezeResponseController.js");

let payroll = require("./src/payroll/payroll.js");

exports.handler = async function (event) {
  let orgId, fileId, action;
  // Validate request (query parameters, requestbody)
  try {
    let queryJSON = JSON.parse(JSON.stringify(event.queryStringParameters));
    if (!neritoUtils.isValidJson(event.body)) {
      return neritoUtils.errorResponseJson("Body json is invalid", 400);
    }
    let json = JSON.parse(event.body);
    if (
      neritoUtils.isEmpty(queryJSON) ||
      neritoUtils.isEmpty(queryJSON["action"])
    ) {
      return neritoUtils.errorResponseJson("Action is not defined", 400);
    }

    if (neritoUtils.isEmpty(json)) {
      return neritoUtils.errorResponseJson("Body is not Defined", 400);
    }

    action = queryJSON["action"];
    orgId = json["orgId"];
    fileId = json["fileId"];

    // Validate actions i.e. (INSERT = save employee, FREEZE = freeze AC data, FREEZE_PAYROLL =freeze PP data, FREEZE_RESPONSE = update bank response)
    if (action.localeCompare(constant.action.INSERT) == 0) {
      if (neritoUtils.isEmpty(json["orgId"])) {
        return neritoUtils.errorResponseJson("orgId Not Found", 400);
      }
      if (neritoUtils.isEmpty(fileId)) {
        return neritoUtils.errorResponseJson("fileId Not Found", 400);
      }
      try {
        const result = await employee(orgId, fileId);
        return result;
      } catch (err) {
        console.error("Something went wrong", err);
        throw err;
      }
    } else if (
      action.localeCompare(constant.action.FREEZE) == 0 ||
      action.localeCompare(constant.action.FREEZE_PAYROLL) == 0
    ) {
      if (neritoUtils.isEmpty(json["orgId"])) {
        return neritoUtils.errorResponseJson("orgId Not Found", 400);
      }
      try {
        const result = await freezeController(orgId, action);
        return result;
      } catch (err) {
        console.error("Something went wrong", err);
        throw err;
      }
    } else if (action.localeCompare(constant.action.PAYROLL_INSERT) == 0) {
      if (neritoUtils.isEmpty(json["orgId"])) {
        return neritoUtils.errorResponseJson("orgId Not Found", 400);
      }
      try {
        const result = await payroll(orgId, fileId);
        return result;
      } catch (err) {
        console.error("Something went wrong", err);
        throw err;
      }
    } else if (action.localeCompare(constant.action.FREEZE_RESPONSE) == 0) {
      try {
        const result = await freezeResponseController.freezeResponse(json);
        return result;
      } catch (err) {
        console.error("Something went wrong", err);
        throw err;
      }
    } else {
      return neritoUtils.errorResponseJson(
        "Preferred Action Is Not Defined",
        400
      );
    }
  } catch (err) {
    console.error("Something went wrong", err);
    return neritoUtils.errorResponseJson(err, 500);
  }
};
