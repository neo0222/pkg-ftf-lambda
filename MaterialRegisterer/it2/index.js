const AWS = require('aws-sdk');
const docClient = new AWS.DynamoDB.DocumentClient({
  region: 'ap-northeast-1'
});

const envSuffix = process.env['ENVIRONMENT'];

const productTableName = 'PRODUCT_' + envSuffix;
const ingredientTableName = 'INGREDIENT_' + envSuffix;
const materialTableName = 'MATERIAL_' + envSuffix;
const sequenceTableName = 'SEQUENCE_' + envSuffix;

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
  const response = {
      statusCode: 200,
      body: {
      },
      headers: {
          "Access-Control-Allow-Origin": '*'
      }
  };
  await handleMaterialOperation(event);
  return response;
}

async function handleMaterialOperation(event) {
  // todo: implement
  switch (event.operation) {
    case 'register':
      await putMaterial(materialTableName, event.item);
      break;
    case 'update':
      await updateMaterial(materialTableName, event.item);
      break;
  }
}


/**
* 食材を新規登録する。
* 
* @param tableName テーブル名
* @param item 登録する商品情報
*/
async function putMaterial(tableName, item) {
  const id = await findNextSequence(tableName);
  const itemToBePut = {
    id: id,
    wholesaler_id: optional(item.wholesaler_id),
    name: optional(item.name),
    price_per_order: optional(item.price_per_order),
    order: {
      amount_per_order: optional(item.amount_per_order),
      order_unit: optional(item.order_unit)
    },
    count: {
      count_per_order: optional(item.count_per_order),
      count_unit: optional(item.count_unit)
    },
    measure: {
      measure_per_order: optional(item.measure_per_order),
      measure_unit: optional(item.measure_unit)
    },
    minimum: {
      minimum_amount: optional(item.minimum_amount),
      minimum_amount_unit: optional(item.minimum_amount_unit)
    },
    proper: {
      proper_amount: optional(item.proper_amount),
      proper_amount_unit: optional(item.proper_amount_unit)
    },
    related_ingredient_list: [],
    is_active: true,
    is_deleted: false
  };
  const params = {
    TableName: tableName,
    Item: itemToBePut
  };
  try {
    await docClient.put(params).promise();
    console.log(`[SUCCESS] registered material data`);
    await updateSequence(tableName);
  }
  catch(error) {
    console.log(`[ERROR] failed to register material data`);
    console.error(error);
    throw error;
  }
}

/**
* 食材の情報を更新する。
* 
* @param tableName テーブル名
* @param item 登録する商品情報
*/
async function updateMaterial(tableName, item) {
  // 更新対象の食材を用いる材料およびその材料を用いる商品の原価を連鎖的に更新する。
  await updateRelatedIngredientAndProductForCost(item);
  const itemToBeUpdated = {
    id: item.id,
    wholesaler_id: optional(item.wholesaler_id),
    name: optional(item.name),
    price_per_order: optional(item.price_per_order),
    order: {
      amount_per_order: optional(item.amount_per_order),
      order_unit: optional(item.order_unit)
    },
    count: {
      count_per_order: optional(item.count_per_order),
      count_unit: optional(item.count_unit)
    },
    measure: {
      measure_per_order: optional(item.measure_per_order),
      measure_unit: optional(item.measure_unit)
    },
    minimum: {
      minimum_amount: optional(item.minimum_amount),
      minimum_amount_unit: optional(item.minimum_amount_unit)
    },
    proper: {
      proper_amount: optional(item.proper_amount),
      proper_amount_unit: optional(item.proper_amount_unit)
    },
    related_ingredient_list: item.related_ingredient_list
  };
  
  const params = {
    TableName: tableName,
    Item: itemToBeUpdated
  };
  
  try {
    await docClient.put(params).promise();
    console.log(`[SUCCESS] updated material data`);
  }
  catch(error) {
    console.log(`[ERROR] failed to update material data`);
    console.error(error);
    throw error;
  }
}

/**
* 更新対象の食材を用いる材料およびその材料を用いる商品の原価を連鎖的に更新する。
* 
* @param tableName テーブル名
* @param item 登録する食材情報
*/
async function updateRelatedIngredientAndProductForCost(item) {
  let ingredientToBeUpdatedInfoList = [];
  let productToBeUpdatedInfoList = [];
  const newProductInfoList = [];
  // price_per_order, measure_per_prepareの変更を関連材料のレシピのcostに反映させる
  await updateRelatedIngredientCost(item, ingredientToBeUpdatedInfoList);
  // 材料のレシピの変更をprice_per_prepareに反映させる
  await updatePricePerPrepare(ingredientToBeUpdatedInfoList, productToBeUpdatedInfoList);
  // price_per_prepareの変更を関連商品のレシピのcostに反映させる
  await calcProductCostByNewIngredientCost(productToBeUpdatedInfoList, newProductInfoList);
  // 商品のレシピの変更を商品の原価に反映させる
  await updateProductCost(newProductInfoList);
}

/**
* 材料の原価が変更された際に、その材料に関連する原価変更などの処理を行う。
* 
* @param productToBeUpdatedInfoList 何らかの材料の情報が更新された商品に関する情報
*/
async function updateRelatedProductForCost(productToBeUpdatedInfoList) {
  const newProductInfoList = [];
  // price_per_prepareの変更を関連商品のレシピのcostに反映させる
  await calcProductCostByNewIngredientCost(productToBeUpdatedInfoList, newProductInfoList);
  // 商品のレシピの変更を商品の原価に反映させる
  await updateProductCost(newProductInfoList);
}

/**
* 
* 
* @param item 更新される食材に関する情報
* @param ingredientToBeUpdatedInfoList 更新される食材を用いる材料を格納していく配列。
*/
async function updateRelatedIngredientCost(item, ingredientToBeUpdatedInfoList) {
  const params = {
    TableName: materialTableName,
    Key: {
      'id': item.id
    }
  };
  const result = await docClient.get(params).promise();
  const obtainedMaterial = result.Item;
  const relatedIngredientList = obtainedMaterial.related_ingredient_list;
  const pricePerOrder = obtainedMaterial.price_per_order;
  const measurePerOrder = obtainedMaterial.measure.measure_per_order;
  if (pricePerOrder === item.price_per_order && measurePerOrder === item.measure_per_order) {
    // コストに関わる変更がなければ処理を抜ける
    console.log('no material info related cost changed.');
    return;
  }
  for (const ingredient of relatedIngredientList) {
    const params = {
      TableName: ingredientTableName,
      Key: {
        'id': ingredient.id
      }
    };
    const result = await docClient.get(params).promise();
    const obtainedIngredient = result.Item;
    
    obtainedIngredient.required_material_list.forEach(material => {
      if (material.id === item.id) {
        // 食材のコストを更新
        if (convertNum(item.measure_per_order) === 0) {
          material.cost = 0;
        } else {
          material.cost = (convertNum(item.price_per_order) * convertNum(material.amount) / convertNum(item.measure_per_order)).toString();
        }
      }
    });
    obtainedIngredient.price_per_prepare = obtainedIngredient.required_material_list.filter(material => material.is_active).map(material => material.cost).reduce((acc, cur) => {
       return convertNum(acc) + convertNum(cur);
    }).toString();
    ingredientToBeUpdatedInfoList.push(obtainedIngredient);
  }
  console.log(`summary: ingredient to be updated : ${JSON.stringify(ingredientToBeUpdatedInfoList)}`);
}

/**
* 食材の情報が更新された際にその食材を用いる材料の原価情報を更新する
* 
* @param ingredientToBeUpdatedInfoList
* @param productToBeUpdatedInfoLit
*/
async function updatePricePerPrepare(ingredientToBeUpdatedInfoList, productToBeUpdatedInfoList) {
  for (const item of ingredientToBeUpdatedInfoList) {
    const params = {
      TableName: ingredientTableName,
      Item: item
    };
    await docClient.put(params).promise();
    productToBeUpdatedInfoList.push({
      ingredientId: item.id,
      pricePerPrepare: item.price_per_prepare,
      measurePerPrepare: item.measure.measure_per_prepare,
      relatedProductIdList: item.related_product_list.map(productInfo => productInfo.id)
    });
  }
  console.log(`updated ingredient: ${JSON.stringify(productToBeUpdatedInfoList)}`);
}

/**
* 材料の原価更新時に、その材料を用いる商品の更新後の原価を算出する。
* 
* @param productToBeUpdatedInfoList 何らかの材料の情報が更新された商品に関する情報
* @param newProductInfoList 更新情報に基づいて原価を再計算した商品の情報からなる配列
*/
async function calcProductCostByNewIngredientCost(productToBeUpdatedInfoList, newProductInfoList) {
  const obtainedProductList = [];
  // ほんとはここは非同期で処理したい。。。
  for (const info of productToBeUpdatedInfoList) {
    for (const productId of info.relatedProductIdList) {
      const params = {
        TableName: productTableName,
        Key: {
          'id': productId
        }
      };
      const result = await docClient.get(params).promise();
      console.log(result.Item);
      obtainedProductList.push(result.Item);
    }
  }
  const obtainedProductListWithNoDuplication = Array.from(new Set(obtainedProductList));
  console.log(JSON.stringify(obtainedProductListWithNoDuplication));
  
  for (const info of productToBeUpdatedInfoList) {
    console.log(`START update product related to ingredient: ${info.ingredientId}`);
    const ingredientId = info.ingredientId;
    const pricePerPrepare = info.pricePerPrepare;
    const measurePerPrepare = info.measurePerPrepare;
    for (const product of obtainedProductListWithNoDuplication) {
      product.required_ingredient_list.forEach(ingredient => {
        if (ingredient.id === ingredientId) {
          // 材料のコストを更新
          console.log(pricePerPrepare, ingredient.amount, measurePerPrepare);
          if (convertNum(measurePerPrepare) === 0) {
            ingredient.cost = 0;
          } else {
            ingredient.cost = (convertNum(pricePerPrepare) * convertNum(ingredient.amount) / convertNum(measurePerPrepare)).toString();
          }
        }
      });
      product.cost = product.required_ingredient_list.filter(ingredient => ingredient.is_active).map(ingredient => ingredient.cost).reduce((acc, cur) => {
        return convertNum(acc) + convertNum(cur);
      }).toString();
      console.log(`calculated new cost of product: ${JSON.stringify(product)}`);
    }
  }
  for (const newProductInfo of obtainedProductListWithNoDuplication) {
    newProductInfoList.push(newProductInfo);
  }
}

/**
* 新たに算出した商品原価情報に基づいて、商品の原価を更新する。
* 
* @param newProductInfoList 更新情報に基づいて原価を再計算した商品の情報からなる配列
*/
async function updateProductCost(newProductInfoList) {
  console.log(JSON.stringify((newProductInfoList)));
  for (const item of newProductInfoList) {
    const params = {
      TableName: productTableName,
      Item: item
    };
    await docClient.put(params).promise();
  }
  console.log(`updated products: ${JSON.stringify(newProductInfoList)}`);
}

async function findNextSequence(targetTableName) {
  const params = {
      TableName: sequenceTableName,
      Key: {
        'table_name': targetTableName
      }
  };
  try {
    const result = await docClient.get(params).promise();
    if (result.Item) {
      return result.Item.next_sequence;
    }
  }
  catch(error) {
    console.error(`[ERROR] failed to retrieve food data`);
    console.error(error);
    throw error;
  }
  const paramsForPutNewRecord = {
    TableName: sequenceTableName,
    Item: {
      table_name: targetTableName,
      current_sequence: 0,
      next_sequence: 1
    }
  };
  try {
    await docClient.put(paramsForPutNewRecord).promise();
    return 1;
  }
  catch (error) {
    console.error(error);
    throw error;
  }
}

async function updateSequence(targetTableName) {
  const params = {
    TableName: sequenceTableName,
    Key: {
      'table_name': targetTableName
    },
    ExpressionAttributeNames: {
      '#n': 'next_sequence',
      '#c': 'current_sequence'
    },
    UpdateExpression: "SET #c = #c + :incr, #n = #n + :incr",
    ExpressionAttributeValues: { 
      ":incr": 1
    },
    ReturnValues: "UPDATED_NEW"
  };
  try {
    const result = await docClient.update(params).promise();
    console.error(`[SUCCESS] updated sequence ${JSON.stringify(result)}`);
  }
  catch(error) {
    console.error(`[ERROR] failed to retrieve food data`);
    console.error(error);
    throw error;
  }
}

function optional(object) {
  return object ? object : undefined;
}

function convertNum(object) {
  return object ? Number(object) : 0;
}

function removeEmptyString(object) {
  const keys = Object.keys(object).filter(key => key !== 'is_active' && key !== 'is_deleted');
  for (const key of keys) {
    object[key] = optional(object[key]);
  }
}