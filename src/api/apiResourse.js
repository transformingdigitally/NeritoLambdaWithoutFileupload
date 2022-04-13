const axios = require("axios");

exports.postApiClient = async function (url, axiosConfig, data) {
  try {
    const response = await axios.post(url, data, axiosConfig);
    return response;
  } catch (err) {
    throw err;
  }
};
