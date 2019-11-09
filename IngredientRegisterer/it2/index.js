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
  await handleIngredientOperation(event);
  return response;
}

async function handleIngredientOperation(event) {
  // todo: implement
  switch (event.operation) {
    case 'register':
      await putIngredient(ingredientTableName, event.payload);
      break;
    case 'update':
      await updateIngredient(ingredientTableName, event.payload);
      break;
  }
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

async function putIngredient(tableName, payload) {
  const id = await findNextSequence(tableName);
  console.log(id);
  const info = payload.ingredientInfo;
  info.id = id;
  const recipe = payload.recipe;
  // is_activeフラグtrueでセット
  recipe.forEach(material => material.is_active = true);
  let itemToBePut;
  if (info.preparation_type === "process_material") {
    // 原価の計算
    const { cost, newRecipe } = await calcCostForIngredient(recipe, info);
    itemToBePut = {
      id: id,
      name: optional(info.name),
      omitted_name: optional(info.omitted_name),
      preparation_type: optional(info.preparation_type),
      required_material_list: newRecipe,
      price_per_prepare: cost,
      prepare: {
        amount_per_prepare: optional(info.amount_per_prepare),
        prepare_unit: optional(info.prepare_unit)
      },
      measure: {
        measure_per_prepare: optional(info.measure_per_prepare),
        measure_unit: optional(info.measure_unit)
      },
      minimum: {
        minimum_amount: optional(info.minimum_amount),
        minimum_amount_unit: optional(info.minimum_amount_unit)
      },
      proper: {
        proper_amount: optional(info.proper_amount),
        proper_amount_unit: optional(info.proper_amount_unit)
      },
      related_product_list: [],
      related_base_item_list: [],
      related_material: undefined,
      is_active: true,
      is_deleted: false
    };
  } else {
    itemToBePut = {
      id: id,
      name: optional(info.name),
      omitted_name: optional(info.omitted_name),
      preparation_type: optional(info.preparation_type),
      required_material_list: [],
      price_per_prepare: undefined,
      prepare: {
        amount_per_prepare: optional(info.amount_per_prepare),
        prepare_unit: optional(info.prepare_unit)
      },
      measure: {
        measure_per_prepare: optional(info.measure_per_prepare),
        measure_unit: optional(info.measure_unit)
      },
      minimum: {
        minimum_amount: optional(info.minimum_amount),
        minimum_amount_unit: optional(info.minimum_amount_unit)
      },
      proper: {
        proper_amount: optional(info.proper_amount),
        proper_amount_unit: optional(info.proper_amount_unit)
      },
      related_product_list: [],
      related_material: info.related_material,
      is_active: true,
      is_deleted: false
    };
  }
  
  const params = {
    TableName: tableName,
    Item: itemToBePut
  };
  console.log(JSON.stringify(params));
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


async function updateIngredient(tableName, payload) {
  const info = payload.ingredientInfo;
  const recipe = payload.recipe;
  let itemToBePut;
  if (info.preparation_type === "process_material") {
    // 原価の計算
    const { cost, newRecipe } = await calcCostForIngredient(recipe, info);
    info.price_per_prepare = cost;
    await asyncUpdateRelatedBaseItemAndProductForCost(info);
    itemToBePut = {
      id: info.id,
      name: optional(info.name),
      omitted_name: optional(info.omitted_name),
      preparation_type: optional(info.preparation_type),
      required_material_list: newRecipe,
      price_per_prepare: cost,
      prepare: {
        amount_per_prepare: optional(info.amount_per_prepare),
        prepare_unit: optional(info.prepare_unit)
      },
      measure: {
        measure_per_prepare: optional(info.measure_per_prepare),
        measure_unit: optional(info.measure_unit)
      },
      minimum: {
        minimum_amount: optional(info.minimum_amount),
        minimum_amount_unit: optional(info.minimum_amount_unit)
      },
      proper: {
        proper_amount: optional(info.proper_amount),
        proper_amount_unit: optional(info.proper_amount_unit)
      },
      related_product_list: info.related_product_list,
      related_base_item_list: info.related_base_item_list,
      related_material: undefined,
      is_active: true,
      is_deleted: false
    };
  } else {
    itemToBePut = {
      id: info.id,
      name: optional(info.name),
      omitted_name: optional(info.omitted_name),
      preparation_type: optional(info.preparation_type),
      required_material_list: [],
      price_per_prepare: undefined,
      prepare: {
        amount_per_prepare: optional(info.amount_per_prepare),
        prepare_unit: optional(info.prepare_unit)
      },
      measure: {
        measure_per_prepare: optional(info.measure_per_prepare),
        measure_unit: optional(info.measure_unit)
      },
      minimum: {
        minimum_amount: optional(info.minimum_amount),
        minimum_amount_unit: optional(info.minimum_amount_unit)
      },
      proper: {
        proper_amount: optional(info.proper_amount),
        proper_amount_unit: optional(info.proper_amount_unit)
      },
      related_product_list: info.related_product_list,
      related_base_item_list: info.related_base_item_list,
      related_material: info.related_material,
      is_active: true,
      is_deleted: false
    };
  }
  
  const params = {
    TableName: tableName,
    Item: itemToBePut
  };
  console.log(JSON.stringify(params));
  try {
    await docClient.put(params).promise();
    console.log(`[SUCCESS] registered material data`);
  }
  catch(error) {
    console.log(`[ERROR] failed to register material data`);
    console.error(error);
    throw error;
  }
}

/**
* 更新対象の材料を用いる商品ベースと商品および商品ベースを用いる商品の原価を連鎖的かつ非同期に更新する。
* 
* @param tableName テーブル名
* @param item 登録する食材情報
*/
async function asyncUpdateRelatedBaseItemAndProductForCost(item) {
  let updatedIngredientList = [{
    ingredientId: item.id,
    pricePerPrepare: item.price_per_prepare,
    measurePerPrepare: item.measure_per_prepare,
    relatedProductIdList: item.related_product_list.map(productInfo => productInfo.id),
    relatedBaseItemIdList: item.related_base_item_list.map(baseItemInfo => baseItemInfo.id)
  }];
  const baseItemToBeUpdatedWithIngredientInfoList = []
  const productToBeUpdatedWithIngredientInfoList = [];
  const productToBeUpdatedWithBaseItemInfoList = []
  const updatedBaseItemInfoList = []

  const promises = []

  promises.push((async () => {
    // ##### 3. 材料　→　商品ベース ######################################

    // price_per_prepareの変更を関連商品のレシピのcostに反映させる
    await calcBaseItemCostByNewIngredientCost(updatedIngredientList, baseItemToBeUpdatedWithIngredientInfoList);
    // 商品のレシピの変更を商品の原価に反映させる
    await updateBaseItemWithIngredientCost(baseItemToBeUpdatedWithIngredientInfoList, updatedBaseItemInfoList);

  })())

  promises.push((async () => {
    // ##### 4. 材料　→　商品 ###########################################

    // price_per_prepareの変更を関連商品のレシピのcostに反映させる
    await calcProductCostByNewIngredientCost(updatedIngredientList, productToBeUpdatedWithIngredientInfoList);
    // 商品のレシピの変更を商品の原価に反映させる
    await updateProductCost(productToBeUpdatedWithIngredientInfoList);

  })())

  try {
    await Promise.all(promises);
  }
  catch (error) {
    throw error
  }
  
  // ##### 5. 商品ベース　→　商品 ###########################################

  // price_per_prepareの変更を関連商品のレシピのcostに反映させる
  await calcProductCostByNewBaseItemCost(updatedBaseItemInfoList, productToBeUpdatedWithBaseItemInfoList);
  // 商品のレシピの変更を商品の原価に反映させる
  await updateProductWithBasePriceCost(productToBeUpdatedWithBaseItemInfoList);
}

/**
* 材料の原価更新時に、その材料を用いる商品ベースの更新後の原価を算出する。
* 
* @param BaseItemToBeUpdatedInfoList 何らかの材料の情報が更新された商品ベースに関する情報
* @param baseItemToBeUpdatedWithIngredientInfoList 更新情報に基づいて原価を再計算した商品ベースの情報からなる配列
*/
async function calcBaseItemCostByNewIngredientCost(updatedIngredientList, baseItemToBeUpdatedWithIngredientInfoList) {
  const obtainedBaseItemList = [];
  // 非同期処理
  const promises = [];
  for (const info of updatedIngredientList) {
    for (const baseItemId of info.relatedBaseItemIdList) {
      promises.push((async () => {
        const params = {
          TableName: baseItemTableName,
          Key: {
            'id': baseItemId
          }
        };
        const result = await docClient.get(params).promise();
        obtainedBaseItemList.push(result.Item);
      })())
    }
  }

  try {
    await Promise.all(promises);
  }
  catch (error) {
    throw error;
  }

  const obtainedBaseItemListWithNoDuplication = Array.from(new Set(obtainedBaseItemList));
  
  for (const info of updatedIngredientList) {
    console.log(`START update base item related to ingredient: ${info.ingredientId}`);
    const ingredientId = info.ingredientId;
    const pricePerPrepare = info.pricePerPrepare;
    const measurePerPrepare = info.measurePerPrepare;
    const promises = []
    for (const baseItem of obtainedBaseItemListWithNoDuplication) {
      promises.push((async () => {
        for (const ingredient of baseItem.required_ingredient_list) {
          if (ingredient.id === ingredientId) {
            // 材料のコストを更新
            if (convertNum(measurePerPrepare) === 0) {
              ingredient.cost = 0;
            } else {
              ingredient.cost = (convertNum(pricePerPrepare) * convertNum(ingredient.amount) / convertNum(measurePerPrepare)).toString();
            }
          }
        };
        baseItem.price_per_prepare = 
          (
            convertNum(
              baseItem.required_ingredient_list
                .filter(ingredient => ingredient.is_active)
                .map(ingredient => ingredient.cost)
                .reduce((acc, cur) => {
                  return convertNum(acc) + convertNum(cur);
              }, 0)
            )
            +
            convertNum(
              baseItem.required_material_list
                .filter(material => material.is_active)
                .map(material => material.cost)
                .reduce((acc, cur) => {
                  return convertNum(acc) + convertNum(cur);
              }, 0)
            )
          
          ).toString();
      })())
    }

    try {
      await Promise.all(promises);
    }
    catch (error) {
      throw error;
    }

  }
  for (const newBaseItemInfo of obtainedBaseItemListWithNoDuplication) {
    baseItemToBeUpdatedWithIngredientInfoList.push(newBaseItemInfo);
  }
  console.log(`summary: base items to be updated are ${JSON.stringify(baseItemToBeUpdatedWithIngredientInfoList.map(base => base.id))}`);
}

/**
* 新たに算出した商品ベース原価情報に基づいて、商品ベースの原価を更新する。
* 
* @param baseItemToBeUpdatedWithIngredientInfoList 更新情報に基づいて原価を再計算した商品の情報からなる配列
*/
async function updateBaseItemWithIngredientCost(baseItemToBeUpdatedWithIngredientInfoList, updatedBaseItemInfoList) {
  const promises = [];
  for (const item of baseItemToBeUpdatedWithIngredientInfoList) {
    promises.push((async () => {
      const params = {
        TableName: baseItemTableName,
        Item: item
      };
      await docClient.put(params).promise();
      updatedBaseItemInfoList.push({
        baseItemId: item.id,
        pricePerPrepare: item.price_per_prepare,
        measurePerPrepare: item.measure.measure_per_prepare,
        relatedProductIdList: item.related_product_list.map(productInfo => productInfo.id)
      });
    })());
  }

  try {
    await Promise.all(promises);
  }
  catch (error) {
    throw error;
  }

  console.log(`updated base items: ${JSON.stringify(baseItemToBeUpdatedWithIngredientInfoList)}`);
}

/**
* 材料の原価更新時に、その材料を用いる商品の更新後の原価を算出する。
* 
* @param updatedIngredientList 何らかの材料の情報が更新された商品に関する情報
* @param productToBeUpdatedWithIngredientInfoList 更新情報に基づいて原価を再計算した商品の情報からなる配列
*/
async function calcProductCostByNewIngredientCost(updatedIngredientList, productToBeUpdatedWithIngredientInfoList) {
  const obtainedProductList = [];
  // ほんとはここは非同期で処理したい。。。
  const promises = []
  const productIdListWithNoDuplication = [];
  const isAlreadyObtained = (id) => productIdListWithNoDuplication.includes(id);

  for (const info of updatedIngredientList) {
    for (const productId of info.relatedProductIdList) {
      if (isAlreadyObtained(productId)) continue;
      productIdListWithNoDuplication.push(productId);
    }
  }

  for (const productId of productIdListWithNoDuplication) {
    promises.push((async () => {
      const params = {
        TableName: productTableName,
        Key: {
          'id': productId
        }
      };
      const result = await docClient.get(params).promise();
      obtainedProductList.push(result.Item);
    })());
  }

  
  try {
    await Promise.all(promises);
  }
  catch (error) {
    throw error;
  }

  const obtainedProductListWithNoDuplication = Array.from(new Set(obtainedProductList));

  console.log(`summary: products to be updated is ${JSON.stringify(obtainedProductListWithNoDuplication.map(product => product.id))}`);

  for (const info of updatedIngredientList) {
    console.log(`START update product related to ingredient: ${info.ingredientId}`);
    const ingredientId = info.ingredientId;
    const pricePerPrepare = info.pricePerPrepare;
    const measurePerPrepare = info.measurePerPrepare;
    for (const product of obtainedProductListWithNoDuplication) {
      for (const ingredient of product.required_ingredient_list) {
        if (ingredient.id === ingredientId) {
          // 材料のコストを更新
          if (convertNum(measurePerPrepare) === 0) {
            ingredient.cost = 0;
          } else {
            ingredient.cost = (convertNum(pricePerPrepare) * convertNum(ingredient.amount) / convertNum(measurePerPrepare)).toString();
          }
        }
      };
      product.cost =
        (
          convertNum(
            product.required_ingredient_list
              .filter(ingredient => ingredient.is_active)
              .map(ingredient => ingredient.cost)
              .reduce((acc, cur) => {
                return convertNum(acc) + convertNum(cur);
              }, 0)
          )
          +
          convertNum(
            product.required_base_item_list
              .filter(baseItem => baseItem.is_active)
              .map(baseItem => baseItem.cost)
              .reduce((acc, cur) => {
                return convertNum(acc) + convertNum(cur);
            }, 0)
          )
        ).toString();
    }
  }
  for (const newProductInfo of obtainedProductListWithNoDuplication) {
    productToBeUpdatedWithIngredientInfoList.push(newProductInfo);
  }
  console.log(`summary: products to be updated with ingredints are ${JSON.stringify(productToBeUpdatedWithIngredientInfoList.map(prod => prod.id))}`);
}

/**
* 新たに算出した商品ベース原価情報に基づいて、商品ベースの原価を更新する。
* 
* @param baseItemToBeUpdatedWithIngredientInfoList 更新情報に基づいて原価を再計算した商品の情報からなる配列
*/
async function updateBaseItemWithIngredientCost(baseItemToBeUpdatedWithIngredientInfoList, updatedBaseItemInfoList) {
  const promises = [];
  for (const item of baseItemToBeUpdatedWithIngredientInfoList) {
    promises.push((async () => {
      const params = {
        TableName: baseItemTableName,
        Item: item
      };
      await docClient.put(params).promise();
      updatedBaseItemInfoList.push({
        baseItemId: item.id,
        pricePerPrepare: item.price_per_prepare,
        measurePerPrepare: item.measure.measure_per_prepare,
        relatedProductIdList: item.related_product_list.map(productInfo => productInfo.id)
      });
    })());
  }

  try {
    await Promise.all(promises);
  }
  catch (error) {
    throw error;
  }

  console.log(`updated base items: ${JSON.stringify(baseItemToBeUpdatedWithIngredientInfoList)}`);
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

/**
* 材料の原価更新時に、その材料を用いる商品の更新後の原価を算出する。
* 
* @param updatedBaseItemInfoList 何らかの材料の情報が更新された商品に関する情報
* @param productToBeUpdatedWithBaseItemInfoList 更新情報に基づいて原価を再計算した商品の情報からなる配列
*/
async function calcProductCostByNewBaseItemCost(updatedBaseItemInfoList, productToBeUpdatedWithBaseItemInfoList) {
  const obtainedProductList = [];
  const promises = [];
  const productIdListWithNoDuplication = [];
  const isAlreadyObtained = (id) => productIdListWithNoDuplication.includes(id);

  for (const info of updatedBaseItemInfoList) {
    for (const productId of info.relatedProductIdList) {
      if (isAlreadyObtained(productId)) continue;
      productIdListWithNoDuplication.push(productId);
    }
  }

  // ほんとはここは非同期で処理したい。。。
  for (const productId of productIdListWithNoDuplication) {
    promises.push((async () => {
      const params = {
        TableName: productTableName,
        Key: {
          'id': productId
        }
      };
      const result = await docClient.get(params).promise();
      obtainedProductList.push(result.Item);
    })());
  }

  try {
    await Promise.all(promises);
  }
  catch (error) {
    throw error;
  }

  const obtainedProductListWithNoDuplication = Array.from(new Set(obtainedProductList));

  console.log(`summary: products to be updated with base items is ${JSON.stringify(obtainedProductListWithNoDuplication.map(product => product.id))}`);

  for (const info of updatedBaseItemInfoList) {
    console.log(`START update product related to base item: ${info.baseItemId}`);
    const baseItemId = info.baseItemId;
    const pricePerPrepare = info.pricePerPrepare;
    const measurePerPrepare = info.measurePerPrepare;
    for (const product of obtainedProductListWithNoDuplication) {
      product.required_base_item_list.forEach(baseItem => {
        if (baseItem.id === baseItemId) {
          // 商品ベースのコストを更新
          if (convertNum(measurePerPrepare) === 0) {
            baseItem.cost = 0;
          } else {
            baseItem.cost = (convertNum(pricePerPrepare) * convertNum(baseItem.amount) / convertNum(measurePerPrepare)).toString();
          }
        }
      });
      product.cost = 
        (
          convertNum(
            product.required_base_item_list
            .filter(baseItem => baseItem.is_active)
            .map(baseItem => baseItem.cost)
            .reduce((acc, cur) => {
              return convertNum(acc) + convertNum(cur);
            }, 0)
          )
          +
          convertNum(
            product.required_ingredient_list
            .filter(ingredient => ingredient.is_active)
            .map(ingredient => ingredient.cost)
            .reduce((acc, cur) => {
              return convertNum(acc) + convertNum(cur);
            }, 0)
          )
        ).toString();
    }
  }
  for (const newProductInfo of obtainedProductListWithNoDuplication) {
    productToBeUpdatedWithBaseItemInfoList.push(newProductInfo);
  }
  console.log(`summary: products to be updated are ${JSON.stringify(productToBeUpdatedWithBaseItemInfoList.map(prod => prod.id))}`);
}

/**
* 新たに算出した商品原価情報に基づいて、商品の原価を更新する。
* 
* @param productToBeUpdatedWithIngredientInfoList 更新情報に基づいて原価を再計算した商品の情報からなる配列
*/
async function updateProductCost(productToBeUpdatedWithIngredientInfoList) {
  const promises = []
  for (const item of productToBeUpdatedWithIngredientInfoList) {
    promises.push((async () => {
      const params = {
        TableName: productTableName,
        Item: item
      };
      await docClient.put(params).promise();
    })());
  }

  try {
    await Promise.all(promises);
  }
  catch (error) {
    throw error;
  }

  console.log(`updated products: ${JSON.stringify(productToBeUpdatedWithIngredientInfoList)}`);
}

/**
* 新たに算出した商品原価情報に基づいて、商品の原価を更新する。
* 
* @param productToBeUpdatedWithBaseItemInfoList 更新情報に基づいて原価を再計算した商品の情報からなる配列
*/
async function updateProductWithBasePriceCost(productToBeUpdatedWithBaseItemInfoList) {
  const promises = [];
  for (const item of productToBeUpdatedWithBaseItemInfoList) {
    promises.push((async () => {
      const params = {
        TableName: productTableName,
        Item: item
      };
      await docClient.put(params).promise();
    })())
  }

  try {
    await Promise.all(promises);
  }
  catch (error) {
    throw error
  }
  console.log(`updated products: ${JSON.stringify(productToBeUpdatedWithBaseItemInfoList)}`);
}

async function calcCostForIngredient(recipe, info) {
  let cost = 0;
  const spentMaterialList = [];
  const promises = [];
  for (const material of recipe) {
    promises.push((async () => {
      // 食材データ取得
      const params = {
        TableName: materialTableName,
        Key: {
          'id': material.id
        }
      };
      console.log(params);
      const result = await docClient.get(params).promise();
      const obtainedMaterial = result.Item;
      spentMaterialList.push({
        obtainedMaterial: obtainedMaterial,
        material: material
      });
      
      let costPerMaterial;

      // material.active = trueの場合のみ原価を計上する
      if (material.is_active) {
        const spentAmount = convertNum(material.amount);
        const measurePerOrder = convertNum(result.Item.measure ? result.Item.measure.measure_per_order : undefined);
        const pricePerOrder = convertNum(result.Item.price_per_order);
        costPerMaterial = measurePerOrder === 0 ? 0 : (pricePerOrder * spentAmount) / measurePerOrder;
        material.cost = costPerMaterial.toString();
      }
      return new Promise((resolve) => resolve(costPerMaterial))
    })())
  }

  try {
    const materialCostList = await Promise.all(promises);
    cost = materialCostList.reduce((acc, cur) => { return acc + cur}).toString();
  }
  catch (error) {
    throw error;
  }

  // 関連食材の更新
  // 今回使わなくなった食材を抽出activate = falseに
  const unspentMaterialList = await obtainUnspentMaterial(info.id, recipe);
  await updateRelatedIngredient(unspentMaterialList, spentMaterialList, info.id, info.preparation_type, info.name);

  // レシピのデータソース＝使われなくなりinactiveになった材料 ＋ 続投 ＋ 新規材料
  return  { cost: cost.toString(), newRecipe: unspentMaterialList.concat(recipe) };
}

/**
* 材料更新により使われなくなった食材を以前のレシピから抽出する。
* 
* @param ingredientId
* @param newRecipe
* @return 使われなくなった食材
*/
async function obtainUnspentMaterial(ingredientId, newRecipe) {
  const params = {
    TableName: ingredientTableName,
    Key: {
      'id': ingredientId
    }
  };
  const result = await docClient.get(params).promise();
  if (!result.Item) {
    return [];
  }
  const prevRecipe = result.Item.required_material_list;
  // レシピの材料のうちoldにあってnewにないものだけ集める
  const unspentMaterialList = prevRecipe.filter(oldMaterial => !newRecipe.some(newMaterial => newMaterial.id === oldMaterial.id));
  unspentMaterialList.forEach(material => {
    material.is_active = false;
  });
  return unspentMaterialList;
}

/**
* 材料更新により使われなくなった食材を以前のレシピから抽出する。
* 
* @param unspentMaterialList
* @param newlySpentMaterialList { obtainedMaterial: DBから取得したmaterial, material: レシピの材料としてのmaterial }
* @return 使われなくなった食材
*/
async function updateRelatedIngredient(unspentMaterialList, spentMaterialList, ingredientId, preparationType, ingredientName) {
  const relatedIngredientListToBeUpdateList = [];
  // unspentの処理
  const promises = [];
  for (const unspentMaterial of unspentMaterialList) {
    promises.push((async () => {
      // レシピから取得したMaterialのデータを元に、DBから食材データを取得
      const params = {
        TableName: materialTableName,
        Key: {
          'id': unspentMaterial.id
        }
      };
      console.log(params);
      const result = await docClient.get(params).promise();
      const prevRelatedIngredientList = result.Item.related_ingredient_list;
      // active = falseに（1種類の食材に対してactiveなものが1つしかないという前提で）
      prevRelatedIngredientList.forEach(ingredient => {
        if (ingredient.id === ingredientId && ingredient.is_active) {
          ingredient.is_active = false;
        }
      });
      relatedIngredientListToBeUpdateList.push({
        materialId: unspentMaterial.id,
        newRelatedIngredientList: prevRelatedIngredientList
      });
    })());
  }

  try {
    await Promise.all(promises);
  }
  catch (error) {
    throw error;
  }

  // newlyの処理
  for (const spentMaterial of spentMaterialList) {
    const isAlreadySpent = spentMaterial.obtainedMaterial.related_ingredient_list.some(ingredient => ingredient.id === ingredientId);
    if (!isAlreadySpent) {// 新たな材料の場合
      const relatedIngredientList = spentMaterial.obtainedMaterial.related_ingredient_list;
      relatedIngredientList.push({
        id: ingredientId,
        name: ingredientName,
        amount: spentMaterial.material.amount,
        measure_unit: spentMaterial.material.measure_unit,
        preparation_type: preparationType,
        is_active: spentMaterial.material.is_active
      });
      relatedIngredientListToBeUpdateList.push({
        materialId: spentMaterial.material.id,
        newRelatedIngredientList: relatedIngredientList
      });
      continue;
    }
    const relatedIngredientList = spentMaterial.obtainedMaterial.related_ingredient_list;
    const activeIngredientIndex = relatedIngredientList.findIndex(ingredient => ingredient.id === ingredientId);
    if (activeIngredientIndex > -1 && spentMaterial.material.is_active) {// 変更の場合
      relatedIngredientList[activeIngredientIndex] = {
        id: ingredientId,
        name: ingredientName,
        amount: spentMaterial.material.amount,
        measure_unit: spentMaterial.material.measure_unit,
        preparation_type: preparationType,
        is_active: spentMaterial.material.is_active
      };
      relatedIngredientListToBeUpdateList.push({
        materialId: spentMaterial.material.id,
        newRelatedIngredientList: relatedIngredientList
      });
    }
  }
  // 更新
  for (const relatedIngredientList of relatedIngredientListToBeUpdateList) {
    for (const ingredient of relatedIngredientList.newRelatedIngredientList) {
      removeEmptyString(ingredient);
    }
    const params = {
      TableName: materialTableName,
      Key: {
        'id': relatedIngredientList.materialId
      },
      ExpressionAttributeNames: {
        "#relatedIngredientList": "related_ingredient_list"
      },
      ExpressionAttributeValues: {
        ":relatedIngredientList": relatedIngredientList.newRelatedIngredientList
      },
      UpdateExpression: "SET #relatedIngredientList = :relatedIngredientList"
    };
    await docClient.update(params).promise();
  }
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