const AWS = require('aws-sdk');
const docClient = new AWS.DynamoDB.DocumentClient({
  region: 'ap-northeast-1'
});

const envSuffix = process.env['ENVIRONMENT'];

const productTableName = 'PRODUCT_' + envSuffix;
const ingredientTableName = 'INGREDIENT_' + envSuffix;
const materialTableName = 'MATERIAL_' + envSuffix;
const sequenceTableName = 'SEQUENCE_' + envSuffix;
const baseItemTableName = 'BASE_ITEM_' + envSuffix;
const stockTableName = 'STOCK_' + envSuffix;

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
    // case 'update':
    //   await updateProduct(event.payload);
    //   break;
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
    KeyConditionExpression: "#foodType = :foodType",
    ExpressionAttributeNames:{
        "#foodType": "food_type"
    },
    ExpressionAttributeValues: {
        ":foodType": event.foodType
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
      'food_type': event.foodType === '食材' ? 'material': 'ingredient',
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
    TableName: materialTableName,
    Key: {
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
    console.log(material.measure.measure_per_order, payload.amount, convertNum(material.order.amount_per_order));
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
    TableName: ingredientTableName,
    Key: {
      'id': payload.id
    }
  };
  const result = await docClient.get(params).promise();
  const ingredient = result.Item;

  if (payload.stockType === '仕込み単位残数') {
    if (!ingredient.prepare.amount_per_prepare) return;
    stock.prepare.amount_per_prepare = payload.amount;
    stock.measure.measure_per_order = calcStockAmount(ingredient.prepare.amount_per_prepare, payload.amount, ingredient.measure.measure_per_prepare);
  } else if (payload.stockType === '計量単位残数') {
    if (!ingredient.measure.measure_per_prepare) return;
    stock.measure.measure_per_order = payload.amount;
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
    return (convertNum(stockAmountInput) * convertNum(targetBasedAmount) / convertNum(basedAmount)).toString();
  } else {
    return undefined;
  }
}

function convertNum(object) {
  return object ? Number(object) : 0;
}