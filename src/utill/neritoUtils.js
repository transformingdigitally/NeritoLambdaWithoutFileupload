const constant = require("../constants/constant");

module.exports = {
  successResponseJson: async function (message, code) {
    let response = {};
    let responseBody = {};
    responseBody.Success = message;
    response.isBase64Encoded = false;
    response.statusCode = code;
    (response.headers = {
      "X-Requested-With": "*",
      "Access-Control-Allow-Headers":
        "Content-Type,X-Amz-Date,Authorization,X-Api-Key,x-requested-with",
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "POST,GET,OPTIONS,PUT",
    }),
      (response.body = JSON.stringify(responseBody));
    //console.log(response);
    return response;
  },

  errorResponseJson: async function (message, code) {
    let response = {};
    let responseBody = {};
    responseBody.Errors = message;
    response.isBase64Encoded = false;
    (response.headers = {
      "X-Requested-With": "*",
      "Access-Control-Allow-Headers":
        "Content-Type,X-Amz-Date,Authorization,X-Api-Key,x-requested-with",
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "POST,GET,OPTIONS,PUT",
    }),
      (response.statusCode = code);
    response.body = JSON.stringify(responseBody);
    //console.log(response);
    return response;
  },
  dateconverter: function (date) {
    var matches = /^(\d{1,2})[-\/](\d{1,2})[-\/](\d{4})$/.exec(date);
    if (matches == null) return false;
    var m = matches[2] - 1;
    var d = matches[1];
    var y = matches[3];
    var composedDate = new Date(y, m, d);
    return composedDate;
  },
  isValidDate: function (date) {
    var matches = /^(\d{1,2})[-\/](\d{1,2})[-\/](\d{4})$/.exec(date);
    if (matches == null) return false;
    var m = matches[2] - 1;
    var d = matches[1];
    var y = matches[3];
    var composedDate = new Date(y, m, d);
    return (
      composedDate.getDate() == d &&
      composedDate.getMonth() == m &&
      composedDate.getFullYear() == y
    );
  },
  stringToDate: function (_date, _format, _delimiter) {
    var formatLowerCase = _format.toLowerCase();
    var formatItems = formatLowerCase.split(_delimiter);
    var dateItems = _date.split(_delimiter);
    var monthIndex = formatItems.indexOf("mm");
    var dayIndex = formatItems.indexOf("dd");
    var yearIndex = formatItems.indexOf("yyyy");
    var month = parseInt(dateItems[monthIndex]);
    month -= 1;
    var formatedDate = new Date(
      dateItems[yearIndex],
      month,
      dateItems[dayIndex]
    );
    return formatedDate;
  },
  formatDate: function (date) {
    var d = new Date(date),
      month = "" + (d.getMonth() + 1),
      day = "" + d.getDate(),
      year = d.getFullYear();

    if (month.length < 2) month = "0" + month;
    if (day.length < 2) day = "0" + day;

    return [day, month, year].join("-");
  },
  StringDateConverter: function (stringDate) {
    let day = stringDate.substring(0, 2);
    let month = stringDate.substring(2, 4);
    let year = stringDate.substring(4, 8);
    return [day, month, year].join("-");
  },
  dateFormatter: function (d) {
    //get the month
    var month = d.getMonth();
    //get the day
    //convert day to string
    var day = d.getDate().toString();
    //get the year
    var year = d.getFullYear();

    //pull the last two digits of the year
    year = year.toString().substr(-2);

    //increment month by 1 since it is 0 indexed
    //converts month to a string
    month = (month + 1).toString();

    //if month is 1-9 pad right with a 0 for two digits
    if (month.length === 1) {
      month = "0" + month;
    }

    //if day is between 1-9 pad right with a 0 for two digits
    if (day.length === 1) {
      day = "0" + day;
    }

    //return the string "MMddyy"
    return year + month + day;
  },
  appendValueOnLeft: function (str, type, appender) {
    return appendValueOnLeft(str, type, appender);
  },
  spacesAppenderOnRight: function (str, length) {
    let strLength = str.length;
    if (strLength === length) {
      return str;
    }
    return str.toString().padEnd(length);
  },
  isEmpty: function (obj) {
    return isEmpty(obj);
  },
  isEmptyStr: function (obj) {
    return isEmptyStr(obj);
  },
  isValidJson: function (val) {
    try {
      let json = JSON.parse(val);
      if (!isEmpty(json)) {
        return true;
      }
    } catch (err) {
      console.error(err);
      return false;
    }
  },

  fixDecimalPlaces: function (val, type) {
    try {
      let number = parseFloat(val);
      number = number.toFixed(2);
      number = number * 100;
      number = appendValueOnLeft(number, type,"0");

      return number;
    } catch (err) {
      console.error(err);
      return 0;
    }
  },
  addDecimalPlaces: function (val) {
    try {
      if (!isEmptyStr(val)) {
        let number = parseFloat(val);
        number = number.toFixed(2);
        return number;
      } else {
        let number = parseFloat("0.00");
        number = number.toFixed(2);
        return number;
      }
    } catch (err) {
      console.error(err);
      return "0.00";
    }
  },
};
function isEmpty(obj) {
  return obj === null || obj === undefined || Object.keys(obj).length === 0;
}
function isEmptyStr(obj) {
  return obj === null || obj === undefined || obj.length === 0;
}
function appendValueOnLeft(str, type, appender) {
  if (isEmptyStr(str)) {
    return "".toString().padStart(type, appender);
  }
  let length = str.length;
  if (length === type) {
    return str;
  }
  return str.toString().padStart(type, appender);
}
