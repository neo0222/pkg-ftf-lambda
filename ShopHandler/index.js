const AWS = require('aws-sdk');
const docClient = new AWS.DynamoDB.DocumentClient({
  region: 'ap-northeast-1'
});

const envSuffix = process.env['ENVIRONMENT'];
const dateThreshold = process.env['DATE_THRESHOLD'];

const shopTableName = 'SHOP_' + envSuffix;
const businessDateTableName = 'BUSINESS_DATE_' + envSuffix;
const wholesalerTableName = 'WHOLESALER_' + envSuffix;

let response = {
  statusCode: 200,
  body: {},
  headers: {
      "Access-Control-Allow-Origin": '*'
  }
};

exports.handler = async (event, context) => {
  if (event.warmup) {
      console.log("This is warm up.");
  } else {
      console.log(`[event]: ${JSON.stringify(event)}`);
  }
  
  return await main(event, context);
};

async function main(event, context) {
  response = {
    statusCode: 200,
    body: {
      shopList: []
    },
    headers: {
        "Access-Control-Allow-Origin": '*'
    }
  };
  await handleOperation(event);
  return response;
}

async function handleOperation(event) {
  switch (event.operation) {
    case 'register':
      await putShop(event.payload);
      break;
    case 'find-all':
      await findAllShops();
      break;
    case 'create-business-date':
      await createBusinessDate();
      break;
  }
}

async function putShop(payload) {
  return;
}

async function findAllShops() {
  params = {
    TableName: shopTableName
  };
  try {
    const result = await docClient.scan(params).promise();
    for (const shop of result.Items) {
      response.body.shopList.push(shop);
    }
  }
  catch (error) {
    throw error;
  }
}

/**
 * 各店舗毎に営業日データを作成する。
 * 
 */
async function createBusinessDate() {
  // 全店舗の情報をSHOPテーブルから取得
  const shopList = await findAllShop();
  // 各店舗の発注先の情報を取得
  // 各店舗毎に作成済みの最新の日付を取得
  const shopInfoList = await retrieveShopInfo(shopList);
  // 日付を作成
  await createBusinessDateForAllShop(shopInfoList);
}

/**
 * 全店舗の情報を取得する。
 * 
 */
async function findAllShop() {
  const params = {
    TableName: shopTableName
  };
  try {
    const result = await docClient.scan(params).promise();
    return result.Items;
  }
  catch (error) {
    throw error;
  }
}

/**
 * 各店舗の発注先の情報および作成済みの最新の営業日を非同期で取得する。
 * 
 * @param {List<Shop>} shopList 
 * @return 店舗名、発注先リスト、最新の営業日からなるオブジェクトの配列
 */
async function retrieveShopInfo(shopList) {
  try {
    const results = await Promise.all([
      retrieveWholesalerListForAllShops(shopList),
      retrieveLatestBusinessDateForAllShops(shopList)
    ]);
    const WholesalerListMap = results[0];
    const LatestBusinessDateMap = results[1];
    return shopList.map(shop => {
      return {
        name: shop.shop_name,
        wholesalerList: WholesalerListMap[shop.shop_name],
        latestBusinessDate: LatestBusinessDateMap[shop.shop_name]
      }
    });
  }
  catch (error) {
    throw error;
  }
}

/**
 * 全店舗の発注先情報を非同期で取得する。
 * 
 * @param {*} shopList 
 */
async function retrieveWholesalerListForAllShops(shopList) {
  const promises = [];
  let wholesalerListMap = {}
  for (const shop of shopList) {
    promises.push(retrieveWholesalerList(shop.shop_name, wholesalerListMap));
  }
  try {
    await Promise.all(promises);
    return wholesalerListMap;
  }
  catch (error) {
    throw error;
  }
}

/**
 * ある店舗の発注先情報を取得する。
 * @param {*} shop 
 */
async function retrieveWholesalerList(shopName, wholesalerListMap) {
  const params = {
    TableName: wholesalerTableName,
    KeyConditionExpression: "#shopName = :shopName",
    ExpressionAttributeNames:{
      "#shopName": "shop_name"
    },
    ExpressionAttributeValues: {
      ":shopName": shopName
    }
  };
  try {
    const result = await docClient.query(params).promise();
    wholesalerListMap[shopName] = result.Items;
  }
  catch (error) {
    throw error;
  }
}

async function retrieveLatestBusinessDateForAllShops(shopList) {
  const promises = [];
  let latestBusinessDateMap = {}
  for (const shop of shopList) {
    promises.push(retrieveLatestBusinessDate(shop.shop_name, latestBusinessDateMap));
  }
  try {
    await Promise.all(promises);
    return latestBusinessDateMap;
  }
  catch (error) {
    throw error;
  }
}

async function retrieveLatestBusinessDate(shopName, latestBusinessDateMap) {
  const params = {
    TableName: businessDateTableName,
    KeyConditionExpression: "#shopName = :shopName",
    ExpressionAttributeNames:{
      "#shopName": "shop_name"
    },
    ExpressionAttributeValues: {
      ":shopName": shopName
    },
    ScanIndexForward: false,
    Limit: 1
  };
  try {
    const result = await docClient.query(params).promise();
    if (result.Items[0]) {
      latestBusinessDateMap[shopName] = result.Items[0].date
    } else {
      const today = new Date();
      const yesterday = new Date(today.setDate(today.getDate() - 1));
      const yesterdayLocal = new Date(yesterday.setHours(yesterday.getHours() + 9));
      latestBusinessDateMap[shopName] = yesterdayLocal.getFullYear() + '-' + (yesterdayLocal.getMonth() + 1) + '-' + yesterdayLocal.getDate();
    }
  }
  catch (error) {
    throw error;
  }
}

/**
 * 
 * @param {List<shopInfo>} shopInfoList 
 */
async function createBusinessDateForAllShop(shopInfoList) {
  const promises = [];
  for (const shopInfo of shopInfoList) {
    promises.push(createBusinessDateForEachShop(shopInfo));
  }
  try {
    await Promise.all(promises);
  }
  catch (error) {
    throw error;
  }
}

/**
 * 
 * @param {shopInfo} shopInfo
 */
async function createBusinessDateForEachShop(shopInfo) {
  const latestBusinessDate = new Date(shopInfo.latestBusinessDate);
  // 作るか判定
  if (!validateBusinessDate(latestBusinessDate)) {
    console.log(`${shopInfo.name}'s business date has been already created up to threshold.`)
    return;  
  }
  // 最新の営業日に基づいて永続化予定の営業日をNew
  const nextBusinessDate = new Date(latestBusinessDate.setDate(latestBusinessDate.getDate() + 1));
  // 日付をdynamoに保存する形式に変換する
  const businessDateToBeRegistered = nextBusinessDate.toISOString().slice(0, 10);
  // 曜日を取得
  const dayTarget = nextBusinessDate.getDay();
  // 定休の発注先があるかを確認し、該当するものをpush
  const closedWholesalerList = []
  for (const wholesaler of shopInfo.wholesalerList) {
    if (wholesaler.regular_holiday.includes(dayTarget)) {
      closedWholesalerList.push({
        id: wholesaler.id,
        name: wholesaler.name
      });
    }
  }
  // BUSINESS_DATEテーブルにput
  const params = {
    TableName: businessDateTableName,
    Item: {
      shop_name: shopInfo.name,
      date: businessDateToBeRegistered,
      day: dayTarget,
      closed_wholesaler_list: closedWholesalerList
    }
  };
  try {
    await docClient.put(params).promise();
  }
  catch (error) {
    throw error;
  }
}

function validateBusinessDate(businessDate) {
  const today = new Date();
  const threshold = new Date(today.getTime() + dateThreshold*24*60*60*1000);
  return businessDate < threshold;
}