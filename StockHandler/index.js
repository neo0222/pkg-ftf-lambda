const AWS = require('aws-sdk');
const BigNumber = require('bignumber.js');
const docClient = new AWS.DynamoDB.DocumentClient({
  region: 'ap-northeast-1'
});

const envSuffix = process.env['ENVIRONMENT'];
const foodTableName = 'FOOD_' + envSuffix;
const productTableName = 'PRODUCT_' + envSuffix;
const ingredientTableName = 'INGREDIENT_' + envSuffix;
const materialTableName = 'MATERIAL_' + envSuffix;
const sequenceTableName = 'SEQUENCE_' + envSuffix;
const baseItemTableName = 'BASE_ITEM_' + envSuffix;
const stockTableName = 'STOCK_' + envSuffix;
const flowTableName = 'FLOW_' + envSuffix;


let response = {
  statusCode: 200,
  body: {
    stockList: []
  },
  headers: {
      "Access-Control-Allow-Origin": '*'
  }
};

exports.handler = async (event, context) => {
  // TODO implement
    
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
      stockList: []
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
    // case 'register':
    //   await putProduct(event.payload);
    //   break;
    case 'update-by-daily-sales':
      await updateByFlow(event, 'update-by-daily-sales');
      break;
    case 'findAll':
      await getAllStocks(event);
      break;
    case 'register':
      await registerStock(event);
      break;
  }
}

async function getAllStocks(event) {
  const params = {
    TableName: stockTableName,
    KeyConditionExpression: "#shopNameFoodType = :shopNameFoodType",
    ExpressionAttributeNames:{
        "#shopNameFoodType": "shop_name_food_type"
    },
    ExpressionAttributeValues: {
        ":shopNameFoodType": event.shopName + ':' + event.foodType
    }
  };
  try {
    const result = await docClient.query(params).promise();
    console.log(`[SUCCESS] retrieved ${result.Count} data: `);
    if (event.foodType === 'material') {
      for (const item of result.Items) {
        response.body.stockList.push({
          id: item.id,
          name: item.name,
          amount_per_order: item.order.amount_per_order,
          order_unit: item.order.order_unit,
          count_per_order: item.count.count_per_order,
          count_unit: item.count.count_unit,
          measure_per_order: item.measure.measure_per_order,
          measure_unit: item.measure.measure_unit,
          minimum_amount: item.minimum.minimum_amount,
          minimum_amount_unit: item.minimum.minimum_amount_unit,
          proper_amount: item.proper.proper_amount,
          proper_amount_unit: item.proper.proper_amount_unit,
          is_active: item.is_active,
          is_deleted: item.is_deleted
        });
      }
    } else {
      for (const item of result.Items) {
        response.body.stockList.push({
          id: item.id,
          name: item.name,
          amount_per_prepare: item.prepare.amount_per_prepare,
          prepare_unit: item.prepare.prepare_unit,
          measure_per_prepare: item.measure.measure_per_prepare,
          measure_unit: item.measure.measure_unit,
          minimum_amount: item.minimum.minimum_amount,
          minimum_amount_unit: item.minimum.minimum_amount_unit,
          proper_amount: item.proper.proper_amount,
          proper_amount_unit: item.proper.proper_amount_unit,
          is_active: item.is_active,
          is_deleted: item.is_deleted
        });
      }
    }
  }
  catch(error) {
    console.log(`[ERROR] failed to retrieve food data`);
    console.error(error);
    throw error;
  }
}

async function registerStock(event) {
  const result = await docClient.get({
    TableName: stockTableName,
    Key: {
      'shop_name_food_type': event.shopName + ':' + (event.foodType === '食材' ? 'material': 'ingredient'),
      'id': event.payload.id
    }
  }).promise();
  if (event.foodType === '食材') {
    await registerMaterialStock(event, result.Item);
  } else {
    await registerIngredientStock(event, result.Item);
  }
}

async function registerMaterialStock(event, stock) {
  const payload = event.payload;

  const params = {
    TableName: foodTableName,
    Key: {
      'shop_name_food_type': event.shopName + ':material',
      'id': payload.id
    }
  };
  const result = await docClient.get(params).promise();
  const material = result.Item;
  console.log(JSON.stringify(material));
  if (payload.stockType === '発注単位残数') {
    if (!material.order.amount_per_order) return;
    stock.order.amount_per_order = payload.amount;
    stock.count.count_per_order = calcStockAmount(material.order.amount_per_order, payload.amount, material.count.count_per_order);
    stock.measure.measure_per_order = calcStockAmount(material.order.amount_per_order, payload.amount, material.measure.measure_per_order);
  } else if (payload.stockType === '棚卸単位残数') {
    if (!material.count.count_per_order) return;
    stock.count.count_per_order = payload.amount;
    stock.order.amount_per_order = calcStockAmount(material.count.count_per_order, payload.amount, material.order.amount_per_order);
    stock.measure.measure_per_order = calcStockAmount(material.count.count_per_order, payload.amount, material.measure.measure_per_order);
  } else if (payload.stockType === '計量単位残数') {
    if (!material.measure.measure_per_order) return;
    stock.measure.measure_per_order = payload.amount;
    console.log(material.measure.measure_per_order, payload.amount, convertBigNumber(material.order.amount_per_order));
    stock.order.amount_per_order = calcStockAmount(material.measure.measure_per_order, payload.amount, material.order.amount_per_order);
    stock.count.count_per_order = calcStockAmount(material.measure.measure_per_order, payload.amount, material.count.count_per_order);
  }

  await docClient.put({
    TableName: stockTableName,
    Item: stock
  }).promise();

  console.log(`updated stock: ${JSON.stringify({
    TableName: stockTableName,
    Item: stock
  })}`)
}

async function registerIngredientStock(event, stock) {
  const payload = event.payload;

  const params = {
    TableName: foodTableName,
    Key: {
      'shop_name_food_type': event.shopName + ':ingredient',
      'id': payload.id
    }
  };
  const result = await docClient.get(params).promise();
  const ingredient = result.Item;

  if (payload.stockType === '仕込み単位残数') {
    if (!ingredient.prepare.amount_per_prepare) return;
    stock.prepare.amount_per_prepare = payload.amount;
    stock.measure.measure_per_prepare = calcStockAmount(ingredient.prepare.amount_per_prepare, payload.amount, ingredient.measure.measure_per_prepare);
  } else if (payload.stockType === '計量単位残数') {
    if (!ingredient.measure.measure_per_prepare) return;
    stock.measure.measure_per_prepare = payload.amount;
    stock.prepare.amount_per_prepare = calcStockAmount(ingredient.measure.measure_per_prepare, payload.amount, ingredient.prepare.amount_per_prepare);
  }

  await docClient.put({
    TableName: stockTableName,
    Item: stock
  }).promise();
}

/**
* 在庫量を算出する。
* 
* @param basedAmount 入力された在庫種別の基準数量
* @param stockAmountInput 入力された在庫数量
* @param targetBasedAmount 算出対象の在庫種別の基準数量
* @return いずれかが0の時: 0, そうでない時、入力された在庫数量 * 算出対象の在庫種別の基準数量 / 入力された在庫種別の基準数量
*/
function calcStockAmount(basedAmount, stockAmountInput, targetBasedAmount) {
  if (convertNum(basedAmount) && convertNum(stockAmountInput) && convertNum(targetBasedAmount)) {
    return (convertBigNumber(stockAmountInput).times(convertBigNumber(targetBasedAmount)).dividedBy(convertBigNumber(basedAmount))).toFixed(2);
  } else {
    return undefined;
  }
}

/**
 * 消費量に基づいて食材と食材料の在庫残数を減少させる。
 * 
 * @param  event 
 */
async function updateByFlow(event, operation) {
  // パラメータ取得
  const shopName = event.shopName;
  const date = event.payload.date;
  // オペレーションに応じてflowの種別を特定
  const flowType = determineFlowType(operation);

  const materialFlowData = await findFlowByFoodTypeAndDateAndFlowType(shopName, 'material', date, flowType);
  const ingredientFlowData = await findFlowByFoodTypeAndDateAndFlowType(shopName, 'ingredient', date, flowType);

  const maybeSpentMaterialIdList = Object.keys(materialFlowData);
  const maybeSpentIngredientIdList = Object.keys(ingredientFlowData);

  const promises = [];
  for (const materialId of maybeSpentMaterialIdList) {
    promises.push(updateStockByFoodTypeAndId(shopName, 'material', materialId, materialFlowData[materialId]))
  }
  for (const ingredientId of maybeSpentIngredientIdList) {
    promises.push(updateStockByFoodTypeAndId(shopName, 'ingredient', ingredientId, ingredientFlowData[ingredientId]))
  }
  try {
    await Promise.all(promises);
  }
  catch (error) {
    throw error;
  }
}

function determineFlowType(operation) {
  if (operation === 'update-by-daily-sales') {
    return 'daily_sales';
  }
}

async function findFlowByFoodTypeAndDateAndFlowType(shopName, foodType, date, flowType, flowData) {
  const params = {
    TableName: flowTableName,
    Key: {
      'shop_name_food_type': shopName + ':' + foodType,
      'date': date
    }
  };

  try {
    const result = await docClient.get(params).promise();
    console.log(`[SUCCESS]retrieved flow data ${JSON.stringify(result.Item.flow_data)}`)
    return result.Item.flow_data;
  }
  catch (error) {
    throw error;
  }
}

async function updateStockByFoodTypeAndId(shopName, foodType, id, flowData) {

  const stockToBeUpdated = await findStockByFoodTypeAndId(shopName, foodType, id);

  const updatedStock = createNewStock(foodType, flowData, stockToBeUpdated);

  await putUpdatedStock(updatedStock);
}

async function findStockByFoodTypeAndId(shopName, foodType, id) {
  const params = {
    TableName: stockTableName,
    Key: {
      'shop_name_food_type': shopName + ':' + foodType,
      'id': Number(id)
    }
  };

  try {
    const result = await docClient.get(params).promise();
    return result.Item;
  }
  catch (error) {
    throw error;
  }
}

function createNewStock(foodType, flowData, stockToBeUpdated) {
  if (foodType === 'material') {
    return createNewMaterialStock(flowData, stockToBeUpdated);
  } else if (foodType === 'ingredient') {
    return createNewIngredientStock(flowData, stockToBeUpdated);
  }
}

function createNewMaterialStock(flowData, stockToBeUpdated) {
  // 消費前の残数
  const measurePerOrder = convertBigNumber(stockToBeUpdated.measure.measure_per_order);
  const countPerOrder = convertBigNumber(stockToBeUpdated.count.count_per_order);
  const amountPerOrder = convertBigNumber(stockToBeUpdated.order.amount_per_order);

  // 消費した減少差分(これは計量単位)
  const descrement = convertBigNumber(flowData.daily_amount);
  
  if (measurePerOrder === 0) return stockToBeUpdated;

  // 更新
  stockToBeUpdated.measure.measure_per_order = (measurePerOrder.minus(descrement)).toFixed(2);
  stockToBeUpdated.count.count_per_order   = (countPerOrder.minus(countPerOrder.dividedBy(measurePerOrder).times(descrement))).toFixed(2);
  stockToBeUpdated.order.amount_per_order  = (amountPerOrder.minus(amountPerOrder.dividedBy(measurePerOrder).times(descrement))).toFixed(2);

  console.log(stockToBeUpdated)
  return stockToBeUpdated;
}

function createNewIngredientStock(flowData, stockToBeUpdated) {
  console.log(JSON.stringify(stockToBeUpdated));
  // 消費前の残数
  const measurePerPrepare = convertBigNumber(stockToBeUpdated.measure.measure_per_prepare);
  const amountPerPrepare = convertBigNumber(stockToBeUpdated.prepare.amount_per_prepare);

  // 消費した減少差分(これは計量単位)
  const descrement = convertBigNumber(flowData.daily_amount);

  // 更新
  stockToBeUpdated.measure.measure_per_prepare = (measurePerPrepare.minus(descrement)).toFixed(2);
  stockToBeUpdated.prepare.amount_per_prepare  = (amountPerPrepare.minus(amountPerPrepare.dividedBy(measurePerPrepare).times(descrement))).toFixed(2);

  console.log(stockToBeUpdated);
  return stockToBeUpdated;
}

async function putUpdatedStock(updatedStock) {
  const params = {
    TableName: stockTableName,
    Item: updatedStock
  };
  try {
    await docClient.put(params).promise();
  }
  catch (error) {
    throw error;
  }
}

function convertBigNumber(object) {
  return object ? new BigNumber(object) : new BigNumber(0);
}

function convertNum(object) {
  return object ? Number(object) : 0;
}