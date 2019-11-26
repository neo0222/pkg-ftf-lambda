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
      flowList: []
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
    case 'register-daily-amount':
      await registerDailyAmount(event.payload);
      break;
    case 'confirm-daily-amount':
      await confirmDailyAmount(event.payload);
      break;
    case 'confirm-daily-consumption':
      await confirmDailyConsumption(event.payload);
      break;
    case 'findAll':
      await getAllFlows(event);
      break;
  }
}

async function getAllFlows(event) {
  const params = {
    TableName: flowTableName,
    KeyConditionExpression: "#foodType = :foodType and #date = :date",
    ExpressionAttributeNames:{
        "#foodType": "food_type",
        "#date": "date"
    },
    ExpressionAttributeValues: {
        ":foodType": event.foodType,
        ":date": event.date
    }
  };
  console.log(JSON.stringify(params))
  try {
    const result = await docClient.query(params).promise();
    console.log(`[SUCCESS] retrieved ${result.Count} data: `);
      for (const item of result.Items) {
        response.body.flowList.push(item);
      }
  }
  catch(error) {
    console.log(`[ERROR] failed to retrieve flow data`);
    console.error(error);
    throw error;
  }
}


async function registerDailyAmount(payload) {
  const amountInfo = payload.amountInfo;
  let flowData = {}
  for (const info of amountInfo) {
    flowData[info.id.toString()] = {
      daily_amount: optional(info.daily_amount),
      menu_type: optional(info.menu_type) ? optional(info.menu_type) : 'regular' // デフォルトでレギュラーメニュー
    }
  }
  const params = {
    TableName: flowTableName,
    Item: {
      food_type: 'product',
      date: payload.date,
      flow_data: flowData,
      daily_amount_status: 'pending'
    }
  };
  try {
    await docClient.put(params).promise();
    console.log(`[SUCCESS] registered daily flow data ${JSON.stringify(params.Item)}`)
  }
  catch (error) {
    throw error;
  }
}

async function confirmDailyAmount(payload) {
  const amountInfo = payload.amountInfo;
  let flowData = {}
  for (const info of amountInfo) {
    flowData[info.id.toString()] = {
      daily_amount: optional(info.daily_amount),
      menu_type: optional(info.menu_type) ? optional(info.menu_type) : 'regular' // デフォルトでレギュラーメニュー
    }
  }
  const params = {
    TableName: flowTableName,
    Item: {
      food_type: 'product',
      date: payload.date,
      flow_data: flowData,
      daily_amount_status: 'confirmed'
    }
  };
  try {
    await docClient.put(params).promise();
    console.log(`[SUCCESS] confirmed daily flow data ${JSON.stringify(params.Item)}`)
  }
  catch (error) {
    throw error;
  }
}

/**
* 承認済みの日次提供数を取得する。
* 
* @param payload { date: 消費量算出対象の日付}
*/
async function confirmDailyConsumption(payload) {
  console.log(JSON.stringify(payload));
  // 当該の日付のconfirmed product flowを取得
  const productFlow = await obtainConfirmedProductFlow(payload.date);
  // 消費された材料および食材を算出
  const { ingredientFlow, materialFlow } = await calcNewFoodConsumptionFlow(productFlow);
  // 永続化
  await persistFoodConsumptionFlow(ingredientFlow, materialFlow, payload.date);
}

async function obtainConfirmedProductFlow(date) {
  const params = {
    TableName: flowTableName,
    KeyConditionExpression: "#foodType = :foodType and #date = :date",
    ExpressionAttributeNames:{
        "#foodType": "food_type",
        "#date": "date"
    },
    ExpressionAttributeValues: {
        ":foodType": 'product',
        ":date": date
    }
  };
  try {
    const result = await docClient.query(params).promise();
    console.log(`[SUCCESS] retrieved ${result.Count} data: `);
    if (result.Items[0].daily_amount_status !== 'confirmed') {
      throw new Error("daily amount of product is not confirmed.");
    }
    return result.Items[0].flow_data;
  }
  catch(error) {
    console.log(`[ERROR] failed to retrieve flow data`);
    console.error(error);
    throw error;
  }
}

async function calcNewFoodConsumptionFlow(productFlow) {
  // 材料算出（非同期）
  const ingredientFlow = await calcIngredientConsumption(productFlow);
  // 食材算出
  const materialFlow = await calcMaterialConsumption(ingredientFlow);
  
  return { ingredientFlow, materialFlow };
}

async function calcIngredientConsumption(productFlow) {
  let ingredientFlow = {};

  //  材料をpush
  try {
    await Promise.all([
      pushIngredientRequiredForBaseItem(productFlow, ingredientFlow),
      pushIngredientRequiredProduct(productFlow, ingredientFlow)
    ])
  }
  catch (error) {
    throw error;
  }
  
  console.log(`summary: the total amount of ingredients for sales were ${JSON.stringify(ingredientFlow)}`);

  return ingredientFlow;
}

async function pushIngredientRequiredForBaseItem(productFlow, ingredientFlow) {
  const productIdList = obtainFoodIdList(productFlow);
  const promises = [];
  for (const productId of productIdList) {
    promises.push(calcAmountOfBaseItemRequiredForBaseItemRequiredForProduct(productFlow, productId, ingredientFlow));
  }

  try {
    await Promise.all(promises);
  } catch (error) {
    throw error;
  }
}

async function pushIngredientRequiredProduct(productFlow, ingredientFlow) {
  const productIdList = obtainFoodIdList(productFlow);
  const promises = [];
  for (const productId of productIdList) {
    promises.push(calcAmountOfIngredientRequiredForProduct(productFlow, productId, ingredientFlow));
  }

  try {
    await Promise.all(promises);
  } catch (error) {
    throw error;
  }
}

async function calcAmountOfIngredientRequiredForProduct(productFlow, productId, ingredientFlow) {
  return new Promise(async (resolve) => {
    const dailyAmount = productFlow[productId].daily_amount;
    const isRegular = productFlow[productId].menu_type === 'regular';
    const product = await findByFoodByFoodTypeAndId('product', productId);
    const requiredIngredientList = product.required_ingredient_list;
    for (const ingredient of requiredIngredientList) {
      if (!ingredient.is_active) {
        continue;
      }
      if (!ingredientFlow[ingredient.id.toString()]) {
        ingredientFlow[ingredient.id.toString()] = {};
      }
      ingredientFlow[ingredient.id.toString()].daily_amount = 
        (convertNum(ingredientFlow[ingredient.id.toString()].daily_amount) + ( convertNum(ingredient.amount) * convertNum(dailyAmount))).toString();
      if (isRegular) {
        ingredientFlow[ingredient.id.toString()].regular_amount = 
        (convertNum(ingredientFlow[ingredient.id.toString()].regular_amount) + ( convertNum(ingredient.amount) * convertNum(dailyAmount))).toString();
      } else {
        ingredientFlow[ingredient.id.toString()].limited_amount = 
          (convertNum(ingredientFlow[ingredient.id.toString()].limited_amount) + ( convertNum(ingredient.amount) * convertNum(dailyAmount))).toString();
      }
    }
    resolve();
  });
}

async function calcAmountOfBaseItemRequiredForBaseItemRequiredForProduct(productFlow, productId, ingredientFlow) {
  let baseItemFlow = {};
  return new Promise(async (resolve) => {
    const dailyProductAmount = productFlow[productId].daily_amount;
    const isRegular = productFlow[productId].menu_type === 'regular';
    const product = await findByFoodByFoodTypeAndId('product', productId);
    const requiredBaseItemList = product.required_base_item_list;
    for (const baseItem of requiredBaseItemList) {
      if (!baseItem.is_active) {
        continue;
      }
      if (!baseItemFlow[baseItem.id.toString()]) {
        baseItemFlow[baseItem.id.toString()] = {}
      }
      baseItemFlow[baseItem.id.toString()].daily_amount = 
        (convertNum(baseItemFlow[baseItem.id.toString()].daily_amount) + ( convertNum(baseItem.amount) * convertNum(dailyProductAmount))).toString();
      if (isRegular) {
        baseItemFlow[baseItem.id.toString()].regular_amount = 
          (convertNum(baseItemFlow[baseItem.id.toString()].regular_amount) + ( convertNum(baseItem.amount) * convertNum(dailyProductAmount))).toString();
      } else {
        baseItemFlow[baseItem.id.toString()].limited_amount = 
          (convertNum(baseItemFlow[baseItem.id.toString()].limited_amount) + ( convertNum(baseItem.amount) * convertNum(dailyProductAmount))).toString();
      }
    }
    console.log(`summary: the amount of base item for product (id=${productId}) were ${JSON.stringify(baseItemFlow)}`);
    await pushIngredientRequiredForBaseItemWithCollectedBaseItem(baseItemFlow, ingredientFlow)
    resolve();
  });
}

async function pushIngredientRequiredForBaseItemWithCollectedBaseItem(baseItemFlow, ingredientFlow) {
  const baseItemIdList = obtainFoodIdList(baseItemFlow);
  const promises = [];
  for (const baseItemId of baseItemIdList) {
    promises.push(calcAmountOfIngredientRequiredForBaseItem(baseItemFlow, baseItemId, ingredientFlow));
  }

  try {
    await Promise.all(promises);
  } catch (error) {
    throw error;
  }
}

async function calcAmountOfIngredientRequiredForBaseItem(baseItemFlow, baseItemId, ingredientFlow) {
  return new Promise(async (resolve) => {
    const dailyAmount = baseItemFlow[baseItemId].daily_amount;
    const limitedAmount = baseItemFlow[baseItemId].limited_amount;
    const regularAmount = baseItemFlow[baseItemId].regular_amount;
    const baseItem = await findByFoodByFoodTypeAndId('base-item', baseItemId);
    const requiredIngredientList = baseItem.required_ingredient_list;
    for (const ingredient of requiredIngredientList) {
      if (!ingredient.is_active) {
        continue;
      }
      if (!ingredientFlow[ingredient.id.toString()]) {
        ingredientFlow[ingredient.id.toString()] = {};
      }
      ingredientFlow[ingredient.id.toString()].daily_amount = 
        (convertNum(ingredientFlow[ingredient.id.toString()].daily_amount) + ( convertNum(ingredient.amount) * convertNum(dailyAmount))).toString();
      ingredientFlow[ingredient.id.toString()].regular_amount = 
        (convertNum(ingredientFlow[ingredient.id.toString()].regular_amount) + ( convertNum(ingredient.amount) * convertNum(regularAmount))).toString();
      ingredientFlow[ingredient.id.toString()].limited_amount = 
        (convertNum(ingredientFlow[ingredient.id.toString()].limited_amount) + ( convertNum(ingredient.amount) * convertNum(limitedAmount))).toString();
      }
    resolve();
  });
}

function obtainFoodIdList(foodFlow) {
  return Object.keys(foodFlow);
}

async function findByFoodByFoodTypeAndId(foodType, idStr) {
  const params = {
    TableName: getTableName(foodType),
    Key: {
      "id": convertNum(idStr)
    }
  };
  try {
    const result = await docClient.get(params).promise();
    return result.Item;
  } catch (error) {
    throw error;
  }
} 

function getTableName(foodType) {
  if (foodType === 'product') {
    return productTableName;
  } else if (foodType === 'ingredient') {
    return ingredientTableName;
  } else if (foodType === 'base-item') {
    return baseItemTableName;
  } else if (foodType === 'material') {
    return materialTableName;
  } else {
    throw new Error('invalid food type selected');
  }
}

async function calcMaterialConsumption(ingredientFlow) {
  let materialFlow = {};

  //  材料をpush
  try {
    await pushMaterialRequiredForIngredient(ingredientFlow, materialFlow)
  }
  catch (error) {
    throw error;
  }
  
  console.log(`summary: the total amount of materials for sales were ${JSON.stringify(materialFlow)}`);

  return materialFlow;
}

async function pushMaterialRequiredForIngredient(ingredientFlow, materialFlow) {
  const ingredientIdList = obtainFoodIdList(ingredientFlow);
  const promises = [];
  for (const ingredientId of ingredientIdList) {
    promises.push(calcAmountOfMaterialRequiredForProduct(ingredientFlow, ingredientId, materialFlow));
  }

  try {
    await Promise.all(promises);
  } catch (error) {
    throw error;
  }
}

async function calcAmountOfMaterialRequiredForProduct(ingredientFlow, ingredientId, materialFlow) {
  return new Promise(async (resolve) => {
    const dailyAmount = ingredientFlow[ingredientId].daily_amount;
    const regularAmount = ingredientFlow[ingredientId].regular_amount;
    const limitedAmount = ingredientFlow[ingredientId].limited_amount;
    const ingredient = await findByFoodByFoodTypeAndId('ingredient', ingredientId);
    const requiredMaterialList = ingredient.required_material_list;
    for (const material of requiredMaterialList) {
      if (ingredient.measure.measure_per_prepare === '0' || !ingredient.measure.measure_per_prepare) {
        throw new Error(`ingredient (id=${ingredientId}) has no measure per prepare. please register measure per prepare.`);
      }
      if (!material.is_active) {
        continue;
      }
      if (!materialFlow[material.id.toString()]) {
        materialFlow[material.id.toString()] = {};
      }
      materialFlow[material.id.toString()].daily_amount = 
        (convertNum(materialFlow[material.id.toString()].daily_amount) + ( convertNum(material.amount) * convertNum(dailyAmount) / convertNum(ingredient.measure.measure_per_prepare))).toString();
      materialFlow[material.id.toString()].regular_amount = 
        (convertNum(materialFlow[material.id.toString()].regular_amount) + ( convertNum(material.amount) * convertNum(regularAmount) / convertNum(ingredient.measure.measure_per_prepare))).toString();
      materialFlow[material.id.toString()].limited_amount = 
        (convertNum(materialFlow[material.id.toString()].limited_amount) + ( convertNum(material.amount) * convertNum(limitedAmount) / convertNum(ingredient.measure.measure_per_prepare))).toString();
    }
    resolve();
  });
}


async function persistFoodConsumptionFlow(ingredientFlow, materialFlow, date) {
  try {
    await docClient.put({
      TableName: flowTableName,
      Item: {
        food_type: 'ingredient',
        date: date,
        flow_data: ingredientFlow,
        flow_type: 'daily_sales'
      }
    }).promise();
    await docClient.put({
      TableName: flowTableName,
      Item: {
        food_type: 'material',
        date: date,
        flow_data: materialFlow,
        flow_type: 'daily_sales'
      }
    }).promise();
    console.log(`[SUCCESS] registered daily sales flow data of ingredients and materials. ${JSON.stringify(ingredientFlow)}, ${JSON.stringify(materialFlow)}`);
  }
  catch (error) {
    throw error;
  }
}

function convertNum(object) {
  return object ? Number(object) : 0;
}

function optional(object) {
  return object ? object : undefined;
}