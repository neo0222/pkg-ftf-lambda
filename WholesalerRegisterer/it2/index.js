const AWS = require('aws-sdk');
const docClient = new AWS.DynamoDB.DocumentClient({
  region: 'ap-northeast-1'
});

const envSuffix = process.env['ENVIRONMENT'];

const wholesalerTableName = 'WHOLESALER_' + envSuffix;

exports.handler = async (event, context) => {
  
  if (event.warmup) {
      console.log("This is warm up.");
  } else {
      console.log(`[event]: ${JSON.stringify(event)}`);
  }
  
  return await main(event, context);
};

async function main(event, context) {
  const response = {
      statusCode: 200,
      body: {
      },
      headers: {
          "Access-Control-Allow-Origin": '*'
      }
  };
  await handleWholesalerOperation(event);
  return response;
}

async function handleWholesalerOperation(event) {
  switch (event.operation) {
    case 'update':
      await updateWholesaler(event.payload, event.shopName);
      break;
    case 'register-all':
      await putAllMaterial(foodTableName, event.materialList, event.shopName);
      break;
  }
}

async function updateWholesaler(payload, shopName) {
    payload.shop_name = shopName;
    const params = {
        TableName: wholesalerTableName,
        Item: payload
    };
    try {
        await docClient.put(params).promise();
        console.log(`[SUCCESS] updated wholesaler data ${params}`);
    }
    catch (error) {
        throw error;
    }
}
