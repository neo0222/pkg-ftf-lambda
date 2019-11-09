const AWS = require('aws-sdk');
const docClient = new AWS.DynamoDB.DocumentClient({
  region: 'ap-northeast-1'
});


const envSuffix = process.env['ENVIRONMENT'];

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
      await putBaseItem(baseItemTableName, event.payload);
      break;
    case 'update':
      await updateBaseItem(baseItemTableName, event.payload);
      break;
  }
}

async function putBaseItem(tableName, payload) {
  const id = await findNextSequence(tableName);
  const info = payload.baseItemInfo;
  const recipe = payload.recipe; // ここには食材と材料が混在している
  // is_activeフラグtrueでセット
  recipe.forEach(ingredient => ingredient.is_active = true);
  info.id = id;
  info.is_deleted = false;
  info.is_active = true;
  
  // 材料のみの原価の計算
  const { costForIngredient, newRecipeRelatedToIngredient } = await calcCostForProduct(recipe.filter(ingredient => ingredient.food_type === 'ingredient'), info);
  // 食材のみの原価の計算
  const { costForMaterial, newRecipeRelatedToMaterial } = await calcCostForIngredient(recipe.filter(material => material.food_type === 'material'), info);
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
      is_active: true,
      is_deleted: false
    }
  };
  console.log(params);
  try {
    await docClient.put(params).promise();
    console.info(`[SUCCESS] registered base item data`);
    await updateSequence(tableName);
  }
  catch(error) {
    console.log(`[ERROR] failed to register base item data`);
    console.error(error);
    throw error;
  }
}

async function updateBaseItem(baseItemTableName, payload) {
  
}

async function handleProductOperation(event) {
  switch (event.operation) {
    case 'register':
      await putProduct(productTableName, event.payload);
      break;
    case 'update':
      await updateProduct(productTableName, event.payload);
      break;
  }
}

async function putProduct(tableName, payload) {
  const id = await findNextSequence(tableName);
  const info = payload.productInfo;
  const recipe = payload.recipe;
  // is_activeフラグtrueでセット
  recipe.forEach(ingredient => ingredient.is_active = true);
  info.id = id;
  info.is_deleted = false;
  info.is_active = true;
  // 原価の計算
  const { cost, newRecipe } = await calcCostForProduct(recipe, info);
  info.cost = cost;
  info.required_ingredient_list = newRecipe;
  // 空文字消す
  removeEmptyString(info);
  for (const ingredient of info.required_ingredient_list) {
    removeEmptyString(ingredient);
  }
  const params = {
    TableName: tableName,
    Item: info
  };
  console.log(params);
  try {
    await docClient.put(params).promise();
    console.info(`[SUCCESS] registered product data`);
    await updateSequence(tableName);
  }
  catch(error) {
    console.log(`[ERROR] failed to register product data`);
    console.error(error);
    throw error;
  }
}

async function updateProduct(tableName, payload) {
  const info = payload.productInfo;
  const recipe = payload.recipe;
  
  // 原価の計算
  const { cost, newRecipe } = await calcCostForProduct(recipe, info);
  info.cost = cost;
  info.required_ingredient_list = newRecipe;
  const keys = Object.keys(info).filter(key => key !== 'is_active' && key !== 'is_deleted');
  for (const key of keys) {
    info[key] = optional(info[key]);
  }
  const params = {
    TableName: tableName,
    Item: info
  };
  console.log(JSON.stringify(params));
  try {
    await docClient.put(params).promise();
    console.log(`[SUCCESS] registered product data`);
  }
  catch(error) {
    console.log(`[ERROR] failed to register product data`);
    console.error(error);
    throw error;
  }
}


async function calcCostForProduct(recipe, info) {
  let cost = 0;
  const spentIngredientList = [];
  for (const ingredient of recipe) {
    // 材料データ取得
    const params = {
      TableName: ingredientTableName,
      Key: {
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
  const unspentIngredientList = await obtainUnspentIngredient(info.id, recipe);
  await updateRelatedBaseItemOfIngredient(unspentIngredientList, spentIngredientList, info.id, info.name);
  
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
async function obtainUnspentIngredient(baseItemId, newRecipe) {
  const params = {
    TableName: baseItemTableName,
    Key: {
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
async function updateRelatedBaseItemOfIngredient(unspentIngredientList, spentIngredientList, baseItemId, baseItemName) {
  const relatedBaseItemListToBeUpdateList = [];
  // unspentの処理
  for (const unspentIngredient of unspentIngredientList) {
    // レシピから取得したMaterialのデータを元に、DBから食材データを取得
    const params = {
      TableName: ingredientTableName,
      Key: {
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
      TableName: ingredientTableName,
      Key: {
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
    
    obtainedIngredient.ingredient_recipe.forEach(material => {
      if (material.id === item.id) {
        // 食材のコストを更新
        if (convertNum(item.measure_per_order) === 0) {
          material.cost = 0;
        } else {
          material.cost = (convertNum(item.price_per_order) * convertNum(material.amount) / convertNum(item.measure_per_order)).toString();
        }
      }
    });
    obtainedIngredient.price_per_prepare = obtainedIngredient.ingredient_recipe.filter(material => material.is_active).map(material => material.cost).reduce((acc, cur) => {
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
      ingredient_recipe: newRecipe,
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
      ingredient_recipe: [],
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
    await updateRelatedProductForCost([{
      ingredientId: info.id,
      pricePerPrepare: cost,
      measurePerPrepare: info.measure_per_prepare,
      relatedProductIdList: info.related_product_list.map(product => product.id)
    }]);
    itemToBePut = {
      id: info.id,
      name: optional(info.name),
      omitted_name: optional(info.omitted_name),
      preparation_type: optional(info.preparation_type),
      ingredient_recipe: newRecipe,
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
      ingredient_recipe: [],
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
  }
  catch(error) {
    console.log(`[ERROR] failed to register material data`);
    console.error(error);
    throw error;
  }
}


async function calcCostForIngredient(recipe, info) {
  let cost = 0;
  const spentMaterialList = [];
  for (const material of recipe) {
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
  const unspentMaterialList = await obtainUnspentMaterial(info.id, recipe);
  await updateRelatedIngredient(unspentMaterialList, spentMaterialList, info.id, info.preparation_type, info.name);

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
async function obtainUnspentMaterial(baseItemId, newRecipe) {
  const params = {
    TableName: baseItemTableName,
    Key: {
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
async function updateRelatedIngredient(unspentMaterialList, spentMaterialList, baseItemId, baseItemName) {
  const relatedBaseItemListToBeUpdateList = [];
  // unspentの処理
  for (const unspentMaterial of unspentMaterialList) {
    // レシピから取得したMaterialのデータを元に、DBから食材データを取得
    const params = {
      TableName: materialTableName,
      Key: {
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
      TableName: materialTableName,
      Key: {
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