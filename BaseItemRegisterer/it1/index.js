const AWS = require('aws-sdk');
const docClient = new AWS.DynamoDB.DocumentClient({
  region: 'ap-northeast-1'
});


const envSuffix = process.env['ENVIRONMENT'];

const foodTableName = 'FOOD_' + envSuffix;
const productTableName = 'PRODUCT_' + envSuffix;
const ingredientTableName = 'INGREDIENT_' + envSuffix;
const materialTableName = 'MATERIAL_' + envSuffix;
const baseItemTableName = 'BASE_ITEM_' + envSuffix;
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
  await handleBaseItemOperation(event);
  return response;
}

async function handleBaseItemOperation(event) {
  switch (event.operation) {
    case 'register':
      await putBaseItem(foodTableName, event.payload, event.shopName);
      break;
    case 'update':
      await updateBaseItem(baseItemTableName, event.payload, event.shopName);
      break;
  }
}

async function putBaseItem(tableName, payload, shopName) {
  const id = await findNextSequence(tableName, shopName);
  const info = payload.baseItemInfo;
  const recipe = payload.recipe; // ここには食材と材料が混在している
  // is_activeフラグtrueでセット
  recipe.forEach(ingredient => ingredient.is_active = true);
  info.id = id;
  info.is_deleted = false;
  info.is_active = true;
  
  // 材料のみの原価の計算
  const { costForIngredient, newRecipeRelatedToIngredient } = await calcCostForProduct(recipe.filter(ingredient => ingredient.food_type === 'ingredient'), info, shopName);
  // 食材のみの原価の計算
  const { costForMaterial, newRecipeRelatedToMaterial } = await calcCostForIngredient(recipe.filter(material => material.food_type === 'material'), info, shopName);
  console.log(JSON.stringify(newRecipeRelatedToIngredient));
  console.log(JSON.stringify(newRecipeRelatedToMaterial));
  
  // 合体
  info.cost = (convertNum(costForIngredient) + convertNum(costForMaterial)).toString();
  
  // 空文字消す
  removeEmptyString(info);
  for (const ingredient of newRecipeRelatedToIngredient) {
    removeEmptyString(ingredient);
  }
  for (const material of newRecipeRelatedToMaterial) {
    removeEmptyString(material);
  }
  const params = {
    TableName: tableName,
    Item: {
      shop_name_food_type: shopName + ':base-item',
      id: id,
      name: optional(info.name),
      price_per_prepare: optional(info.cost),
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
      menu_type: optional(info.menu_type),
			product_type_1: optional(info.product_type_1),
			product_type_2: optional(info.product_type_2),
      required_ingredient_list: newRecipeRelatedToIngredient,
      required_material_list: newRecipeRelatedToMaterial,
      related_product_list: [],
      is_active: true,
      is_deleted: false
    }
  };
  console.log(params);
  try {
    await docClient.put(params).promise();
    console.info(`[SUCCESS] registered base item data`);
    await updateSequence(tableName, shopName);
  }
  catch(error) {
    console.log(`[ERROR] failed to register base item data`);
    console.error(error);
    throw error;
  }
}

async function updateBaseItem(tableName, payload, shopName) {
  const info = payload.baseItemInfo;
  const recipe = payload.recipe; // ここには食材と材料が混在している
  
  // 材料のみの原価の計算
  const { costForIngredient, newRecipeRelatedToIngredient } = await calcCostForProduct(recipe.filter(ingredient => ingredient.food_type === 'ingredient'), info, shopName);
  // 食材のみの原価の計算
  const { costForMaterial, newRecipeRelatedToMaterial } = await calcCostForIngredient(recipe.filter(material => material.food_type === 'material'), info, shopName);
  
  // 合体
  info.cost = (convertNum(costForIngredient) + convertNum(costForMaterial)).toString();

  await asyncUpdateRelatedBaseItemAndProductForCost(info, shopName);

  // 空文字消す
  removeEmptyString(info);
  for (const ingredient of newRecipeRelatedToIngredient) {
    removeEmptyString(ingredient);
  }
  for (const material of newRecipeRelatedToMaterial) {
    removeEmptyString(material);
  }

  const params = {
    TableName: foodTableName,
    Item: {
      shop_name_food_type: shopName + ':base-item',
      id: info.id,
      name: optional(info.name),
      price_per_prepare: optional(info.cost),
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
      menu_type: optional(info.menu_type),
			product_type_1: optional(info.product_type_1),
			product_type_2: optional(info.product_type_2),
      required_ingredient_list: newRecipeRelatedToIngredient,
      required_material_list: newRecipeRelatedToMaterial,
      related_product_list: info.related_product_list,
      is_active: true,
      is_deleted: false
    }
  };
  console.log(params);
  try {
    await docClient.put(params).promise();
    console.info(`[SUCCESS] updated base item data`);
  }
  catch(error) {
    console.log(`[ERROR] failed to update base item data`);
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
async function asyncUpdateRelatedBaseItemAndProductForCost(item, shopName) {
  const productToBeUpdatedWithBaseItemInfoList = []
  const updatedBaseItemInfoList = [{
    baseItemId: item.id,
    pricePerPrepare: item.price_per_prepare,
    measurePerPrepare: item.measure_per_prepare,
    relatedProductIdList: item.related_product_list.map(productInfo => productInfo.id)
  }];
  
  // ##### 5. 商品ベース　→　商品 ###########################################

  // price_per_prepareの変更を関連商品のレシピのcostに反映させる
  await calcProductCostByNewBaseItemCost(updatedBaseItemInfoList, productToBeUpdatedWithBaseItemInfoList, shopName);
  // 商品のレシピの変更を商品の原価に反映させる
  await updateProductWithBasePriceCost(productToBeUpdatedWithBaseItemInfoList);
}

/**
* 材料の原価更新時に、その材料を用いる商品の更新後の原価を算出する。
* 
* @param updatedBaseItemInfoList 何らかの材料の情報が更新された商品に関する情報
* @param productToBeUpdatedWithBaseItemInfoList 更新情報に基づいて原価を再計算した商品の情報からなる配列
*/
async function calcProductCostByNewBaseItemCost(updatedBaseItemInfoList, productToBeUpdatedWithBaseItemInfoList, shopName) {
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
        TableName: foodTableName,
        Key: {
          'shop_name_food_type': shopName + ':product',
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
* @param productToBeUpdatedWithBaseItemInfoList 更新情報に基づいて原価を再計算した商品の情報からなる配列
*/
async function updateProductWithBasePriceCost(productToBeUpdatedWithBaseItemInfoList) {
  const promises = [];
  for (const item of productToBeUpdatedWithBaseItemInfoList) {
    promises.push((async () => {
      const params = {
        TableName: foodTableName,
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


async function calcCostForProduct(recipe, info, shopName) {
  let cost = 0;
  const spentIngredientList = [];
  for (const ingredient of recipe) {
    // 材料データ取得
    const params = {
      TableName: foodTableName,
      Key: {
        'shop_name_food_type': shopName + ':ingredient',
        'id': ingredient.id
      }
    };
    console.log(params);
    const result = await docClient.get(params).promise();
    const obtainedIngredient = result.Item;
    spentIngredientList.push({
      obtainedIngredient: obtainedIngredient,
      ingredient: ingredient
    });
    
    // ingredient.active = trueの場合のみ原価を計上する
    if (ingredient.is_active) {
      const spentAmount = convertNum(ingredient.amount);
      const measurePerPrepare = convertNum(result.Item.measure ? result.Item.measure.measure_per_prepare : undefined);
      const pricePerPrepare = convertNum(result.Item.price_per_prepare);
      const costPerIngredient = measurePerPrepare === 0 ? 0 : (pricePerPrepare * spentAmount) / measurePerPrepare;
      console.log(`Ingredient ${ingredient.name} 's cost is ${costPerIngredient} yen.`);
      cost = cost + costPerIngredient;
      ingredient.cost = costPerIngredient.toString();
    }
  }
  // 関連材料の更新
  // 今回使わなくなった材料を抽出activate = falseに
  const unspentIngredientList = await obtainUnspentIngredient(info.id, recipe, shopName);
  await updateRelatedBaseItemOfIngredient(unspentIngredientList, spentIngredientList, info.id, info.name, shopName);
  
  // レシピのデータソース＝使われなくなりinactiveになった材料 ＋ 続投 ＋ 新規材料
  return  { costForIngredient: cost.toString(), newRecipeRelatedToIngredient: unspentIngredientList.concat(recipe) };
}

/**
* 材料更新により使われなくなった材料を以前のレシピから抽出する。
* 
* @param ingredientId
* @param newRecipe
* @return 使われなくなった食材
*/
async function obtainUnspentIngredient(baseItemId, newRecipe, shopName) {
  const params = {
    TableName: foodTableName,
    Key: {
      'shop_name_food_type': shopName + ':base-item',
      'id': baseItemId
    }
  };
  console.log(params);
  const result = await docClient.get(params).promise();
  // putの場合
  if (!result.Item) {
    return [];
  }
  const prevRecipe = result.Item.required_ingredient_list;
  // レシピの材料のうちoldにあってnewにないものだけ集める
  const unspentIngredientList = prevRecipe.filter(oldIngredient => !newRecipe.some(newIngredient => newIngredient.id === oldIngredient.id));
  unspentIngredientList.forEach(ingredient => {
    ingredient.is_active = false;
  });
  console.log(`unspent ingredients are ${JSON.stringify(unspentIngredientList)}`);
  return unspentIngredientList;
}

/**
* 材料データが保持する関連商品ベースリストを更新する。
* 
* @param unspentMaterialList
* @param newlySpentMaterialList { obtainedMaterial: DBから取得したmaterial, material: レシピの材料としてのmaterial }
* @param productId 新たに関連商品として登録する商品のID
* @param productName 新たに関連商品として登録する商品の名称
*/
async function updateRelatedBaseItemOfIngredient(unspentIngredientList, spentIngredientList, baseItemId, baseItemName, shopName) {
  const relatedBaseItemListToBeUpdateList = [];
  // unspentの処理
  for (const unspentIngredient of unspentIngredientList) {
    // レシピから取得したMaterialのデータを元に、DBから食材データを取得
    const params = {
      TableName: foodTableName,
      Key: {
        'shop_name_food_type': shopName + ':ingredient',
        'id': unspentIngredient.id
      }
    };
    const result = await docClient.get(params).promise();
    const prevRelatedBaseItemList = result.Item.related_base_item_list;
    // active = falseに（1種類の材料に対してactiveなものが1つしかないという前提で）
    prevRelatedBaseItemList.forEach(baseItem => {
      if (baseItem.id === baseItemId && baseItem.is_active) {
        baseItem.is_active = false;
      }
    });
    relatedBaseItemListToBeUpdateList.push({
      ingredientId: unspentIngredient.id,
      newRelatedBaseItemList: prevRelatedBaseItemList
    });
  }
  // newlyの処理
  for (const spentIngredient of spentIngredientList) {
    const isAlreadySpent = spentIngredient.obtainedIngredient.related_base_item_list.some(baseItem => baseItem.id === baseItemId);
    if (!isAlreadySpent) {// 新たなbaseItemの場合
      const relatedBaseItemList = spentIngredient.obtainedIngredient.related_base_item_list;
      relatedBaseItemList.push({
        id: baseItemId,
        name: baseItemName,
        amount: spentIngredient.ingredient.amount,
        measure_unit: spentIngredient.ingredient.measure_unit,
        is_active: spentIngredient.ingredient.is_active
      });
      relatedBaseItemListToBeUpdateList.push({
        ingredientId: spentIngredient.ingredient.id,
        newRelatedBaseItemList: relatedBaseItemList
      });
      continue;
    }
    const relatedBaseItemList = spentIngredient.obtainedIngredient.related_base_item_list;
    const activeBaseItemIndex = relatedBaseItemList.findIndex(baseItem => baseItem.id === baseItemId);
    if (activeBaseItemIndex > -1 && spentIngredient.ingredient.is_active) {// 変更の場合
      relatedBaseItemList[activeBaseItemIndex] = {
        id: baseItemId,
        name: baseItemName,
        amount: spentIngredient.ingredient.amount,
        measure_unit: spentIngredient.ingredient.measure_unit,
        is_active: spentIngredient.ingredient.is_active
      };
      relatedBaseItemListToBeUpdateList.push({
        ingredientId: spentIngredient.ingredient.id,
        newRelatedBaseItemList: relatedBaseItemList
      });
    }
  }
  console.log(`changed ingredients: ${JSON.stringify(relatedBaseItemListToBeUpdateList)}`);
  // 更新
  for (const relatedBaseItemList of relatedBaseItemListToBeUpdateList) {
    for (const baseItem of relatedBaseItemList.newRelatedBaseItemList) {
      removeEmptyString(baseItem);
    }
    const params = {
      TableName: foodTableName,
      Key: {
        'shop_name_food_type': shopName + ':ingredient',
        'id': relatedBaseItemList.ingredientId
      },
      ExpressionAttributeNames: {
        "#relatedBaseItemList": "related_base_item_list"
      },
      ExpressionAttributeValues: {
        ":relatedBaseItemList": relatedBaseItemList.newRelatedBaseItemList
      },
      UpdateExpression: "SET #relatedBaseItemList = :relatedBaseItemList"
    };
    await docClient.update(params).promise();
  }
}



async function calcCostForIngredient(recipe, info, shopName) {
  let cost = 0;
  const spentMaterialList = [];
  for (const material of recipe) {
    // 食材データ取得
    const params = {
      TableName: foodTableName,
      Key: {
        'shop_name_food_type': shopName + ':material',
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
    
    // material.active = trueの場合のみ原価を計上する
    if (material.is_active) {
      const spentAmount = convertNum(material.amount);
      const measurePerOrder = convertNum(result.Item.measure ? result.Item.measure.measure_per_order : undefined);
      const pricePerOrder = convertNum(result.Item.price_per_order);
      const costPerMaterial = measurePerOrder === 0 ? 0 : (pricePerOrder * spentAmount) / measurePerOrder;
      cost = cost + costPerMaterial;
      material.cost = costPerMaterial.toString();
    }
  }
  // 関連食材の更新
  // 今回使わなくなった食材を抽出activate = falseに
  const unspentMaterialList = await obtainUnspentMaterial(info.id, recipe, shopName);
  await updateRelatedIngredient(unspentMaterialList, spentMaterialList, info.id, info.preparation_type, info.name, shopName);

  // レシピのデータソース＝使われなくなりinactiveになった材料 ＋ 続投 ＋ 新規材料
  return  { costForMaterial: cost.toString(), newRecipeRelatedToMaterial: unspentMaterialList.concat(recipe) };
}

/**
* 材料更新により使われなくなった食材を以前のレシピから抽出する。
* 
* @param ingredientId
* @param newRecipe
* @return 使われなくなった食材
*/
async function obtainUnspentMaterial(baseItemId, newRecipe, shopName) {
  const params = {
    TableName: foodTableName,
    Key: {
      'shop_name_food_type': shopName + ':base-item',
      'id': baseItemId
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
async function updateRelatedIngredient(unspentMaterialList, spentMaterialList, baseItemId, preparationType, baseItemName, shopName) {
  const relatedBaseItemListToBeUpdateList = [];
  // unspentの処理
  for (const unspentMaterial of unspentMaterialList) {
    // レシピから取得したMaterialのデータを元に、DBから食材データを取得
    const params = {
      TableName: foodTableName,
      Key: {
        'shop_name_food_type': shopName + ':material',
        'id': unspentMaterial.id
      }
    };
    console.log(params);
    const result = await docClient.get(params).promise();
    const prevRelatedBaseItemList = result.Item.related_base_item_list;
    // active = falseに（1種類の食材に対してactiveなものが1つしかないという前提で）
    prevRelatedBaseItemList.forEach(baseItem => {
      if (baseItem.id === baseItemId && baseItem.is_active) {
        baseItem.is_active = false;
      }
    });
    relatedBaseItemListToBeUpdateList.push({
      materialId: unspentMaterial.id,
      newRelatedBaseItemList: prevRelatedBaseItemList
    });
  }
  // newlyの処理
  for (const spentMaterial of spentMaterialList) {
    const isAlreadySpent = spentMaterial.obtainedMaterial.related_base_item_list.some(baseItem => baseItem.id === baseItemId);
    if (!isAlreadySpent) {// 新たな材料の場合
      const relatedBaseItemList = spentMaterial.obtainedMaterial.related_base_item_list;
      relatedBaseItemList.push({
        id: baseItemId,
        name: baseItemName,
        amount: spentMaterial.material.amount,
        measure_unit: spentMaterial.material.measure_unit,
        is_active: spentMaterial.material.is_active
      });
      relatedBaseItemListToBeUpdateList.push({
        materialId: spentMaterial.material.id,
        newRelatedBaseItemList: relatedBaseItemList
      });
      continue;
    }
    const relatedBaseItemList = spentMaterial.obtainedMaterial.related_base_item_list;
    const activeBaseItemIndex = relatedBaseItemList.findIndex(baseItem => baseItem.id === baseItemId);
    if (activeBaseItemIndex > -1 && spentMaterial.material.is_active) {// 変更の場合
      relatedBaseItemList[activeBaseItemIndex] = {
        id: baseItemId,
        name: baseItemName,
        amount: spentMaterial.material.amount,
        measure_unit: spentMaterial.material.measure_unit,
        is_active: spentMaterial.material.is_active
      };
      relatedBaseItemListToBeUpdateList.push({
        materialId: spentMaterial.material.id,
        newRelatedBaseItemList: relatedBaseItemList
      });
    }
  }
  // 更新
  for (const relatedBaseItemList of relatedBaseItemListToBeUpdateList) {
    // 空文字消す
    for (const baseItem of relatedBaseItemList.newRelatedBaseItemList) {
      removeEmptyString(baseItem);
    }
    // 消したら更新処理
    const params = {
      TableName: foodTableName,
      Key: {
        'shop_name_food_type': shopName + ':material',
        'id': relatedBaseItemList.materialId
      },
      ExpressionAttributeNames: {
        "#relatedBaseItemList": "related_base_item_list"
      },
      ExpressionAttributeValues: {
        ":relatedBaseItemList": relatedBaseItemList.newRelatedBaseItemList
      },
      UpdateExpression: "SET #relatedBaseItemList = :relatedBaseItemList"
    };
    await docClient.update(params).promise();
  }
}


async function findNextSequence(targetTableName, shopName) {
  const params = {
      TableName: sequenceTableName,
      Key: {
        'table_name': targetTableName,
        'partition_key': shopName + ':base-item'
      }
  };
  try {
    const result = await docClient.get(params).promise();
    if (result.Item) {
      return result.Item.next_sequence;
    }
  }
  catch(error) {
    console.error(`[ERROR] failed to retrieve next sequence`);
    console.error(error);
    throw error;
  }
  const paramsForPutNewRecord = {
    TableName: sequenceTableName,
    Item: {
      table_name: targetTableName,
      partition_key: shopName + ':base-item',
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

async function updateSequence(targetTableName, shopName) {
  const params = {
    TableName: sequenceTableName,
    Key: {
      'table_name': targetTableName,
      'partition_key': shopName + ':base-item'
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
    console.error(`[ERROR] failed to increment sequence`);
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