const AWS = require('aws-sdk');
const docClient = new AWS.DynamoDB.DocumentClient({
  region: 'ap-northeast-1'
});

const envSuffix = process.env['ENVIRONMENT'];

const wholesalerTableName = 'WHOLESALER_' + envSuffix;
const businessDateTableName = 'BUSINESS_DATE_' + envSuffix;

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

  await updateBusinessDate(shopName);
}

async function updateBusinessDate(shopName) {
  const results = await Promise.all([
    // 発注先情報全取得
    retrieveWholesaler(shopName),
    // 今日以降の営業日データ全取得
    retrieveUpcomingBusinessDate(shopName)
  ]);

  const wholesalerList = results[0];
  const upcomingBusinessDateList = results[1];
  
  // update -> put
  await updateUpcomingBusinessDate(wholesalerList, upcomingBusinessDateList);
}

/**
 * 店舗を指定して全発注先を取得する。
 * 
 * @param {*} shopName 
 */
async function retrieveWholesaler(shopName) {
  const params = {
    TableName: wholesalerTableName,
    KeyConditionExpression: '#shopName = :shopName',
    ExpressionAttributeNames: {
      "#shopName": "shop_name"
    },
    ExpressionAttributeValues: {
      ":shopName": shopName
    }
  };
  try {
    const result = await docClient.query(params).promise();
    return result.Items;
  }
  catch (error) {
    throw error;
  }
}

/**
 * 今日以降の日付の全営業日を取得する。
 * 
 * @param {*} shopName 
 */
async function retrieveUpcomingBusinessDate(shopName) {
  // 今日の日付をYYYY-MM-DDの形式で取得
  const today = new Date();
  const todayLocal = new Date(today.setHours(today.getHours() + 9));
  const todayFormatted = todayLocal.toISOString().slice(0, 10);
  console.log(todayFormatted)
  const params = {
    TableName: businessDateTableName,
    KeyConditionExpression: "#shopName = :shopName and #date > :date",
    ExpressionAttributeNames:{
      "#shopName": "shop_name",
      "#date": "date"
    },
    ExpressionAttributeValues: {
      ":shopName": shopName,
      ":date": todayFormatted
    }
  };
  try {
    const result = await docClient.query(params).promise();
    console.log(JSON.stringify(result.Items))
    return result.Items
  }
  catch (error) {
    throw error;
  }
}

async function updateUpcomingBusinessDate(wholesalerList, upcomingBusinessDateList) {
  const closedWholesalerListMap = bundleClosedWholesaler(wholesalerList);

  const promises = [];

  for (const businessDate of upcomingBusinessDateList) {
    promises.push(putBusinessDate(closedWholesalerListMap[businessDate.day], businessDate));
  }

  try {
    await Promise.all(promises);
  }
  catch (error) {
    throw error;
  }
}

function bundleClosedWholesaler(wholesalerList) {
  let closedWholesalerListMap = {};
  for (let day = 0; day < 8; day++) {
    closedWholesalerListMap[day] = createClosedWholesalerListByDay(wholesalerList, day);
  }
  return closedWholesalerListMap;
}

function createClosedWholesalerListByDay(wholesalerList, day) {
  return wholesalerList
    .filter(wholesaler => wholesaler.regular_holiday.includes(day))
    .map(wholesaler => {
      return {
        id: wholesaler.id,
        name: wholesaler.name
      }
    });
}

async function putBusinessDate(closedWholesalerList, businessDate) {
  businessDate.closed_wholesaler_list = closedWholesalerList;
  const params = {
    TableName: businessDateTableName,
    Item: businessDate
  };

  try {
    await docClient.put(params).promise();
  }
  catch (error) {
    throw error;
  }
}