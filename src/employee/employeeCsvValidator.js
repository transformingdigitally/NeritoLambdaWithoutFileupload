let employeeService = require("../service/employeeService.js");
let service = require("../service/service.js");
let neritoUtils = require("../utill/neritoUtils.js");
let constant = require("../constants/constant.js");

let CSVFileValidator = require("csv-file-validator");
let typeAccountConfig;
let bankIdConfig;
let config = {};

const headers = [
  {
    name: "phoneNumber",
    inputName: "phoneNumber",
    required: false,
    requiredError: function (headerName, rowNumber, columnNumber) {
      return `${rowNumber},  ${headerName} is required in the ${columnNumber} column`;
    },
    unique: false,
    uniqueError: function (headerName, rowNumber) {
      return `${rowNumber},  ${headerName} is not unique`;
    },
    validate: function (phoneNumber) {
      return isPhoneNumberValid(phoneNumber);
    },
    validateError: function (headerName, rowNumber, columnNumber) {
      return `${rowNumber},  ${headerName} is not valid in the ${columnNumber} column`;
    },
  },
  {
    name: "name",
    inputName: "name",
    required: false,
    requiredError: function (headerName, rowNumber, columnNumber) {
      return `${rowNumber},  ${headerName} is required in the ${columnNumber} column`;
    },
    unique: false,
    uniqueError: function (headerName, rowNumber) {
      return `${rowNumber},  ${headerName} is not unique`;
    },
    validate: function (name) {
      return isValidMaxLength("name", name);
    },
    validateError: function (headerName, rowNumber, columnNumber) {
      return `${rowNumber},  ${headerName} is not valid in the ${columnNumber} column`;
    },
  },
  {
    name: "email",
    inputName: "email",
    required: false,
    requiredError: function (headerName, rowNumber, columnNumber) {
      return `${rowNumber},  ${headerName} is required in the ${columnNumber} column`;
    },
    unique: false,
    uniqueError: function (headerName, rowNumber) {
      return `${rowNumber},  ${headerName} is not unique`;
    },
    validate: function (email) {
      return isEmailValid(email);
    },
    validateError: function (headerName, rowNumber, columnNumber) {
      return `${rowNumber},  ${headerName} is not valid in the ${columnNumber} column`;
    },
  },
  {
    name: "contact",
    inputName: "contact",
    required: false,
    requiredError: function (headerName, rowNumber, columnNumber) {
      return `${rowNumber},  ${headerName} is required in the ${columnNumber} column`;
    },
    unique: false,
    uniqueError: function (headerName, rowNumber) {
      return `${rowNumber},  ${headerName} is not unique`;
    },
    validate: function (contact) {
      return isValidMaxLength("contact", contact);
    },
    validateError: function (headerName, rowNumber, columnNumber) {
      return `${rowNumber},  ${headerName} is not valid in the ${columnNumber} column`;
    },
  },
  {
    name: "rfc",
    inputName: "rfc",
    required: false,
    requiredError: function (headerName, rowNumber, columnNumber) {
      return `${rowNumber},  ${headerName} is required in the ${columnNumber} column`;
    },
    unique: false,
    uniqueError: function (headerName, rowNumber) {
      return `${rowNumber},  ${headerName} is not unique`;
    },
    validate: function (rfc) {
      return isValidMaxLength("rfc", rfc);
    },
    validateError: function (headerName, rowNumber, columnNumber) {
      return `${rowNumber},  ${headerName} is not valid in the ${columnNumber} column`;
    },
  },
  {
    name: "typeAccount",
    inputName: "typeAccount",
    required: false,
    requiredError: function (headerName, rowNumber, columnNumber) {
      return `${rowNumber},  ${headerName} is required in the ${columnNumber} column`;
    },
    unique: false,
    uniqueError: function (headerName, rowNumber) {
      return `${rowNumber},  ${headerName} is not unique`;
    },
    validate: function (typeAccount) {
      return isTypeAccountValid(typeAccount);
    },
    validateError: function (headerName, rowNumber, columnNumber) {
      return `${rowNumber},  ${headerName} is not valid in the ${columnNumber} column`;
    },
  },
  {
    name: "bankId",
    inputName: "bankId",
    required: false,
    requiredError: function (headerName, rowNumber, columnNumber) {
      return `${rowNumber},  ${headerName} is required in the ${columnNumber} column`;
    },
    unique: false,
    uniqueError: function (headerName, rowNumber) {
      return `${rowNumber},  ${headerName} is not unique`;
    },
    validate: function (bankId) {
      return isBankIdValid(bankId);
    },
    validateError: function (headerName, rowNumber, columnNumber) {
      return `${rowNumber},  ${headerName} is not valid in the ${columnNumber} column`;
    },
  },
  {
    name: "accountClabe",
    inputName: "accountClabe",
    required: false,
    requiredError: function (headerName, rowNumber, columnNumber) {
      return `${rowNumber},  ${headerName} is required in the ${columnNumber} column`;
    },
    unique: false,
    uniqueError: function (headerName, rowNumber) {
      return `${rowNumber},  ${headerName} is not unique`;
    },
    validate: function (accountClabe) {
      return isValidMaxLength("accountClabe", accountClabe);
    },
    validateError: function (headerName, rowNumber, columnNumber) {
      return `${rowNumber},  ${headerName} is not valid in the ${columnNumber} column`;
    },
  },
];

module.exports = {
  validateCsv: async function (data, orgId) {
    let csvDataResult;
    let orgMetadata = await service.getOrgDataById(orgId);
    if (
      !neritoUtils.isEmpty(orgMetadata) &&
      !neritoUtils.isEmpty(orgMetadata.Items) &&
      !neritoUtils.isEmpty(orgMetadata.Items[0]) &&
      !neritoUtils.isEmpty(orgMetadata.Items[0].FileValidation)
    ) {
      orgMetadata = orgMetadata.Items[0].FileValidation;
      orgMetadata = JSON.parse(JSON.stringify(orgMetadata));
      let ConfigMap = headers.map((obj) => {
        obj.required = orgMetadata[obj.name]["required"];
        obj.unique = orgMetadata[obj.name]["unique"];
        return obj;
      });
      config.headers = ConfigMap;
    } else {
      config.headers = headers;
    }

    typeAccountConfig = await employeeService.getConfigByType(
      constant.config.ACCOUNT_TYPE
    );
    bankIdConfig = await employeeService.getConfigByType(
      constant.config.BANK_ID
    );

    let csvJson = data.Body.toString("utf-8");

    await CSVFileValidator(csvJson, config)
      .then((csvData) => {
        csvDataResult = csvData;
        return csvDataResult;
      })
      .catch((err) => {
        console.error(err);
        return err;
      });
    return csvDataResult;
  },
};

function isEmailValid(email) {
  if (neritoUtils.isEmpty(email)) {
    return true;
  }
  if (email.length > constant.maxLength.EMAIL) {
    return false;
  }
  var valid = constant.emailRegex.test(email);
  if (!valid) {
    return false;
  }
  return true;
}

function isTypeAccountValid(type_account) {
  if (neritoUtils.isEmpty(type_account)) {
    return true;
  }
  let isValid = false;
  if (type_account.length > constant.maxLength.TYPEACCOUNT) {
    return isValid;
  }
  if (neritoUtils.isEmpty(typeAccountConfig)) {
    return isValid;
  }
  let result = JSON.parse(JSON.stringify(typeAccountConfig));
  if (neritoUtils.isEmpty(result)) {
    return isValid;
  }
  let ids = Object.keys(result);
  if (ids.length > 0 && ids.indexOf(type_account) >= 0) {
    isValid = true;
  }

  return isValid;
}

function isBankIdValid(bank_id) {
  if (neritoUtils.isEmpty(bank_id)) {
    return true;
  }
  let isValid = false;
  if (bank_id.length > constant.maxLength.BANKID) {
    return isValid;
  }
  if (neritoUtils.isEmpty(bankIdConfig)) {
    return isValid;
  }
  let result = JSON.parse(JSON.stringify(bankIdConfig));

  if (neritoUtils.isEmpty(result)) {
    return isValid;
  }
  let ids = Object.keys(result);
  if (ids.length > 0 && ids.indexOf(bank_id) >= 0) {
    isValid = true;
  }

  return isValid;
}

function isPhoneNumberValid(phoneNumber) {
  if (neritoUtils.isEmpty(phoneNumber)) {
    return true;
  } else if (phoneNumber.length != constant.maxLength.PHONENUMBER) {
    return false;
  } else if (!constant.phoneNumberRegex.test(phoneNumber)) {
    return false;
  } else {
    return true;
  }
}

function isValidMaxLength(headerName, value) {
  let isValid = true;
  headerName = headerName.toUpperCase().trim();
  if (value.length > constant.maxLength[headerName]) {
    isValid = false;
  }
  return isValid;
}
