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
  item.id = id;
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
    related_base_item_list: [],
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
    await putStock(item);
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
  await asyncUpdateRelatedIngredientAndProductForCost(item);
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
    related_ingredient_list: item.related_ingredient_list,
    related_base_item_list: item.related_base_item_list
  };
  
  const params = {
    TableName: tableName,
    Item: itemToBeUpdated
  };
  
  try {
    await docClient.put(params).promise();
    console.log(`[SUCCESS] updated material data`);
    await updateStock(item);
  }
  catch(error) {
    console.log(`[ERROR] failed to update material data`);
    console.error(error);
    throw error;
  }
}

/**
* 更新対象の食材を用いる材料およびその材料を用いる商品の原価を連鎖的かつ非同期に更新する。
* 
* @param tableName テーブル名
* @param item 登録する食材情報
*/
async function asyncUpdateRelatedIngredientAndProductForCost(item) {
  let ingredientToBeUpdatedInfoList = [];
  let updatedIngredientList = [];
  const baseItemToBeUpdatedWithIngredientInfoList = []
  const baseItemToBeUpedatedWithMaterialInfoList = [];
  const productToBeUpdatedWithIngredientInfoList = [];
  const productToBeUpdatedWithBaseItemInfoList = []
  const updatedBaseItemInfoList = []

  const promises = []

  promises.push((async () => {
    // ##### 1.食材　→　材料 ############################################

    // price_per_order, measure_per_prepareの変更を関連材料のレシピのcostに反映させる
    await calcIngredientCostByNewMaterialCost(item, ingredientToBeUpdatedInfoList);
    // 材料のレシピの変更をprice_per_prepareに反映させる
    await updateIngredientWithMaterialCost(ingredientToBeUpdatedInfoList, updatedIngredientList);

  })())

  promises.push((async () => {
    // ##### 2.食材　→　商品ベース #######################################

    // price_per_order, measure_per_prepareの変更を関連商品ベースのレシピのcostに反映させる
    await calcBaseItemCostByNewMaterialCost(item, baseItemToBeUpedatedWithMaterialInfoList);
    // 商品ベースのレシピの変更をprice_per_prepareに反映させる
    await updateBaseItemWithMaterialCost(baseItemToBeUpedatedWithMaterialInfoList, updatedBaseItemInfoList);
  })())

  try {
    await Promise.all(promises);
  }
  catch (error) {
    throw error
  }

  promises.length = 0

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
* 
* 
* @param item 更新される食材に関する情報
* @param ingredientToBeUpdatedInfoList 更新される食材を用いる材料を格納していく配列。
*/
async function calcIngredientCostByNewMaterialCost(item, ingredientToBeUpdatedInfoList) {
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

  const promises = [

  ]
  for (const ingredient of relatedIngredientList) {
    promises.push((async () => {
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
      }, 0).toString();
      ingredientToBeUpdatedInfoList.push(obtainedIngredient);
    })())
  }
  try {
    await Promise.all(promises)
  }
  catch(error) {
    throw error;
  }
  console.log(`summary: ingredient to be updated : ${JSON.stringify(ingredientToBeUpdatedInfoList.map(ing => ing.id))}`);
}

async function calcBaseItemCostByNewMaterialCost(item, baseItemToBeUpedatedWithMaterialInfoList) {
  const params = {
    TableName: materialTableName,
    Key: {
      'id': item.id
    }
  };
  const result = await docClient.get(params).promise();
  const obtainedMaterial = result.Item;
  const relatedBaseItemList = obtainedMaterial.related_base_item_list;
  const pricePerOrder = obtainedMaterial.price_per_order;
  const measurePerOrder = obtainedMaterial.measure.measure_per_order;
  if (pricePerOrder === item.price_per_order && measurePerOrder === item.measure_per_order) {
    // コストに関わる変更がなければ処理を抜ける
    console.log('no material info related cost changed.');
    return;
  }

  for (const baseItem of relatedBaseItemList) {
    const params = {
      TableName: baseItemTableName,
      Key: {
        'id': baseItem.id
      }
    };
    const result = await docClient.get(params).promise();
    const obtainedBaseItem = result.Item;

    for (const material of obtainedBaseItem.required_material_list) {
      if (material.id === item.id) {
        // 食材のコストを更新
        if (convertNum(item.measure_per_order) === 0) {
          material.cost = 0;
        } else {
          material.cost = (convertNum(item.price_per_order) * convertNum(material.amount) / convertNum(item.measure_per_order)).toString();
        }
      }
    };

    obtainedBaseItem.price_per_prepare = 
      (
        convertNum(
          obtainedBaseItem.required_material_list
            .filter(material => material.is_active)
            .map(material => material.cost)
            .reduce((acc, cur) => {
              return convertNum(acc) + convertNum(cur);
            }, 0) 
        )
        +
        convertNum(
          obtainedBaseItem.required_ingredient_list
            .filter(ingredient => ingredient.is_active)
            .map(ingredient => ingredient.cost)
            .reduce((acc, cur) => {
              return convertNum(acc) + convertNum(cur);
            }, 0)
        )
      ).toString();
    baseItemToBeUpedatedWithMaterialInfoList.push(obtainedBaseItem);
  }
  console.log(`summary: base items to be updated : ${JSON.stringify(baseItemToBeUpedatedWithMaterialInfoList.map(baseItem => baseItem.id))}`);
}

/**
* 食材の情報が更新された際にその食材を用いる材料の原価情報を更新する
* 
* @param ingredientToBeUpdatedInfoList
* @param productToBeUpdatedInfoLit
*/
async function updateIngredientWithMaterialCost(ingredientToBeUpdatedInfoList, updatedIngredientList) {
  const promises = []
  for (const item of ingredientToBeUpdatedInfoList) {
    promises.push((async () => {
      const params = {
        TableName: ingredientTableName,
        Item: item
      };
      await docClient.put(params).promise();
      updatedIngredientList.push({
        ingredientId: item.id,
        pricePerPrepare: item.price_per_prepare,
        measurePerPrepare: item.measure.measure_per_prepare,
        relatedProductIdList: item.related_product_list.map(productInfo => productInfo.id),
        relatedBaseItemIdList: item.related_base_item_list.map(baseItemInfo => baseItemInfo.id)
      });
    })())    
  }

  try {
    await Promise.all(promises);
  }
  catch (error) {
    throw error;
  }

  console.log(`updated ingredient: ${JSON.stringify(updatedIngredientList)}`);
}

/**
* 食材の情報が更新された際にその食材を用いる商品ベースの原価情報を更新する
* 
* @param baseItemToBeUpedatedWithMaterialInfoList
* @param productToBeUpdatedInfoLit
*/
async function updateBaseItemWithMaterialCost(baseItemToBeUpedatedWithMaterialInfoList, updatedBaseItemInfoList) {
  const promises = []
  for (const item of baseItemToBeUpedatedWithMaterialInfoList) {
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
    })())
  }

  try {
    await Promise.all(promises);
  }
  catch (error) {
    throw error;
  }

  console.log(`updated base items: ${JSON.stringify(updatedBaseItemInfoList)}`);
}

/**
* 材料の原価更新時に、その材料を用いる商品ベースの更新後の原価を算出する。
* 
* @param BaseItemToBeUpdatedInfoList 何らかの材料の情報が更新された商品ベースに関する情報
* @param baseItemToBeUpdatedWithIngredientInfoList 更新情報に基づいて原価を再計算した商品ベースの情報からなる配列
*/
async function calcBaseItemCostByNewIngredientCost(updatedIngredientList, baseItemToBeUpdatedWithIngredientInfoList) {
  const obtainedBaseItemList = [];
  // ほんとはここは非同期で処理したい。。。
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

async function putStock(info) {
  const params = {
    TableName: stockTableName,
    Item: {
      id: info.id,
      food_type: 'material',
      measure: {
        measure_unit: optional(info.measure_unit)
      },
      order: {
        order_unit: optional(info.order_unit)
      },
      count: {
        count_unit: optional(info.count_unit)
      },
      minimum: {
        minimum_unit: optional(info.minimum_unit)
      },
      proper: {
        proper_unit: optional(info.proper_unit)
      },
      is_active: true,
      is_deleted: false
    }
  };
  try {
    await docClient.put(params).promise();
  }
  catch (error) {
    throw error;
  }
}

async function updateStock(info) {
  const params = {
    TableName: stockTableName,
    Key: {
      food_type: 'material',
      id: info.id
    }
  };
  const result = await docClient.get(params).promise();
  const stock = result.Item;
  stock.measure.measure_unit = optional(info.measure_unit);
  stock.order.order_unit = optional(info.order_unit);
  stock.count.count_unit = optional(info.count_unit);
  stock.price = (convertNum(info.price_per_order) * convertNum(stock.amount_per_order));

  await docClient.put({
    TableName: stockTableName,
    Item: stock
  }).promise();
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