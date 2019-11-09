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
  await handleProductOperation(event);
  return response;
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
  const requiredBaseItemList = payload.requiredBaseItemList;
  // is_activeフラグtrueでセット
  recipe.forEach(ingredient => ingredient.is_active = true);
  info.id = id;
  info.is_deleted = false;
  info.is_active = true;

  // 原価の計算
  const { costForIngredient, newRecipeRelatedToIngredient } = await calcCostRelatedToIngredient(recipe, info);
  const { costForBaseItem, newRecipeRelatedToBaseItem } = await calcCostRelatedToBaseItem(requiredBaseItemList, info);

  info.cost = (Number(costForIngredient) + Number(costForBaseItem)).toString();
  info.required_ingredient_list = newRecipeRelatedToIngredient;
  info.required_base_item_list = newRecipeRelatedToBaseItem;
  // 空文字消す
  removeEmptyString(info);
  for (const ingredient of info.required_ingredient_list) {
    removeEmptyString(ingredient);
  }
  for (const ingredient of info.required_base_item_list) {
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
  const requiredBaseItemList = payload.requiredBaseItemList;
  
  // 原価の計算
  const { costForIngredient, newRecipeRelatedToIngredient } = await calcCostRelatedToIngredient(recipe, info);
  const { costForBaseItem, newRecipeRelatedToBaseItem } = await calcCostRelatedToBaseItem(requiredBaseItemList, info);

  info.cost = (Number(costForIngredient) + Number(costForBaseItem)).toString();
  info.required_ingredient_list = newRecipeRelatedToIngredient;
  info.required_base_item_list = newRecipeRelatedToBaseItem;

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
    console.log(`[SUCCESS] updated product data`);
  }
  catch(error) {
    console.log(`[ERROR] failed to update product data`);
    console.error(error);
    throw error;
  }
}


async function calcCostRelatedToIngredient(recipe, info) {
  let cost = 0;
  const spentIngredientList = [];
  const promises = []
  for (const ingredient of recipe) {
    // 食材データ取得
    const params = {
      TableName: ingredientTableName,
      Key: {
        'id': ingredient.id
      }
    };
    console.log(params);
    promises.push(obtainIngredientDataAndCalcCost(params, spentIngredientList, ingredient, cost))
  }
  // 関連材料の更新
  // 今回使わなくなった材料を抽出activate = falseに
  try {
    const result = await Promise.all(promises);
  }
  catch (error) {
    throw error;
  }
  const unspentIngredientList = await obtainNewlyUnspentIngredient(info.id, recipe);
  await updateRelatedProduct(unspentIngredientList, spentIngredientList, info.id, info.name);

  // レシピのデータソース＝使われなくなりinactiveになった材料 ＋ 続投 ＋ 新規材料
  return  { costForIngredient: cost.toString(), newRecipeRelatedToIngredient: unspentIngredientList.concat(recipe) };
}

async function obtainIngredientDataAndCalcCost(params, spentIngredientList, ingredient, cost) {
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
/**
* 材料更新により使われなくなった材料を以前のレシピから抽出する。
* 
* @param ingredientId
* @param newRecipe
* @return 使われなくなった食材
*/
async function obtainNewlyUnspentIngredient(productId, newRecipe) {
  const params = {
    TableName: productTableName,
    Key: {
      'id': productId
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
* 材料データが保持する関連商品リストを更新する。
* 
* @param unspentMaterialList
* @param newlySpentMaterialList { obtainedMaterial: DBから取得したmaterial, material: レシピの材料としてのmaterial }
* @param productId 新たに関連商品として登録する商品のID
* @param productName 新たに関連商品として登録する商品の名称
*/
async function updateRelatedProduct(unspentIngredientList, spentIngredientList, productId, productName) {
  const relatedProductListToBeUpdateList = [];
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
    const prevRelatedProductList = result.Item.related_product_list;
    // active = falseに（1種類の材料に対してactiveなものが1つしかないという前提で）
    prevRelatedProductList.forEach(product => {
      if (product.id === productId && product.is_active) {
        product.is_active = false;
      }
    });
    relatedProductListToBeUpdateList.push({
      ingredientId: unspentIngredient.id,
      newRelatedProductList: prevRelatedProductList
    });
  }
  // newlyの処理
  for (const spentIngredient of spentIngredientList) {
    const isAlreadySpent = spentIngredient.obtainedIngredient.related_product_list.some(product => product.id === productId);
    if (!isAlreadySpent) {// 新たな商品の場合
      const relatedProductList = spentIngredient.obtainedIngredient.related_product_list;
      relatedProductList.push({
        id: productId,
        name: productName,
        amount: spentIngredient.ingredient.amount,
        measure_unit: spentIngredient.ingredient.measure_unit,
        is_active: spentIngredient.ingredient.is_active
      });
      relatedProductListToBeUpdateList.push({
        ingredientId: spentIngredient.ingredient.id,
        newRelatedProductList: relatedProductList
      });
      continue;
    }
    const relatedProductList = spentIngredient.obtainedIngredient.related_product_list;
    const activeProductIndex = relatedProductList.findIndex(product => product.id === productId);
    if (activeProductIndex > -1 && spentIngredient.ingredient.is_active) {// 変更の場合
      relatedProductList[activeProductIndex] = {
        id: productId,
        name: productName,
        amount: spentIngredient.ingredient.amount,
        measure_unit: spentIngredient.ingredient.measure_unit,
        is_active: spentIngredient.ingredient.is_active
      };
      relatedProductListToBeUpdateList.push({
        ingredientId: spentIngredient.ingredient.id,
        newRelatedProductList: relatedProductList
      });
    }
  }
  // 更新
  for (const relatedProductList of relatedProductListToBeUpdateList) {
    for (const product of relatedProductList.newRelatedProductList) {
      removeEmptyString(product);
    }
    const params = {
      TableName: ingredientTableName,
      Key: {
        'id': relatedProductList.ingredientId
      },
      ExpressionAttributeNames: {
        "#relatedProductList": "related_product_list"
      },
      ExpressionAttributeValues: {
        ":relatedProductList": relatedProductList.newRelatedProductList
      },
      UpdateExpression: "SET #relatedProductList = :relatedProductList"
    };
    await docClient.update(params).promise();
  }
}

async function calcCostRelatedToBaseItem(recipe, info) {
  let cost = 0;
  const spentBaseItemList = [];
  const promises = []
  for (const baseItem of recipe) {
    // 食材データ取得
    const params = {
      TableName: baseItemTableName,
      Key: {
        'id': baseItem.id
      }
    };
    promises.push((async () => {
      const result = await docClient.get(params).promise();
      const obtainedBaseItem = result.Item;
      spentBaseItemList.push({
        obtainedBaseItem: obtainedBaseItem,
        baseItem: baseItem
      });
      
      // baseItem.active = trueの場合のみ原価を計上する
      if (baseItem.is_required) {
        const spentAmount = convertNum(baseItem.amount);
        const measurePerPrepare = convertNum(result.Item.measure ? result.Item.measure.measure_per_prepare : undefined);
        const pricePerPrepare = convertNum(result.Item.price_per_prepare);
        const costPerBaseItem = measurePerPrepare === 0 ? 0 : (pricePerPrepare * spentAmount) / measurePerPrepare;
        console.log(`Base Item ${baseItem.name} 's cost is ${costPerBaseItem} yen.`);
        cost = cost + costPerBaseItem;
        baseItem.cost = costPerBaseItem.toString();
      }
    })())
    
  }
  try {
    await Promise.all(promises)
  }
  catch (error) {
    throw error;
  }
  // 関連材料の更新
  // 今回使わなくなった材料を抽出activate = falseに
  const unspentBaseItemList = await obtainNewlyUnspentBaseItem(info.id, recipe);
  await updateRelatedProductOfBaseItem(unspentBaseItemList, spentBaseItemList, info.id, info.name);

  const formattedAndActiveRecipe = 
    recipe.filter(baseItem => baseItem.is_active !== undefined || baseItem.is_required) // 一度も入力されていないものとnot requiredのものだけ消す 
      .map(baseItem => {
        return {
          id: baseItem.id,
          name: baseItem.name,
          is_active: baseItem.is_required,
          measure_unit: baseItem.measure_unit,
          amount: baseItem.amount,
          cost: baseItem.cost
        }
      }
    )

  // レシピのデータソース＝使われなくなりinactiveになった材料 ＋ 続投 ＋ 新規材料
  return  { costForBaseItem: cost.toString(), newRecipeRelatedToBaseItem: unspentBaseItemList.concat(formattedAndActiveRecipe) };
}

  /**
* 更新により使われなくなった商品ベースを以前のレシピから抽出する。
* 
* @param ingredientId
* @param newRecipe
* @return 使われなくなった食材
*/
async function obtainNewlyUnspentBaseItem(productId, newRecipe) {
  const params = {
    TableName: productTableName,
    Key: {
      'id': productId
    }
  };
  console.log(params);
  const result = await docClient.get(params).promise();
  // putの場合
  if (!result.Item) {
    return [];
  }
  const prevRecipe = result.Item.required_base_item_list;
  // レシピの材料のうちoldにあってnewにないものだけ集める
  const unspentBaseItemList = 
    prevRecipe.filter(oldBaseItem => {
    }
  );
  unspentBaseItemList.forEach(baseItem => {
    baseItem.is_active = false;
  });
  console.log(`newly unspent base items are ${JSON.stringify(unspentBaseItemList)}`);
  return unspentBaseItemList;
}

/**
* 材料データが保持する関連商品リストを更新する。
* 
* @param unspentMaterialList
* @param newlySpentMaterialList { obtainedMaterial: DBから取得したmaterial, material: レシピの材料としてのmaterial }
* @param productId 新たに関連商品として登録する商品のID
* @param productName 新たに関連商品として登録する商品の名称
*/
async function updateRelatedProductOfBaseItem(unspentBaseItemList, spentBaseItemList, productId, productName) {
  const relatedProductListToBeUpdateList = [];
  // unspentの処理
  for (const unspentBaseItem of unspentBaseItemList) {
    // レシピから取得したMaterialのデータを元に、DBから食材データを取得
    const params = {
      TableName: baseItemTableName,
      Key: {
        'id': unspentBaseItem.id
      }
    };
    const result = await docClient.get(params).promise();
    const prevRelatedProductList = result.Item.related_product_list;
    // active = falseに（1種類の材料に対してactiveなものが1つしかないという前提で）
    prevRelatedProductList.forEach(product => {
      if (product.id === productId && product.is_active) {
        product.is_active = false;
      }
    });
    relatedProductListToBeUpdateList.push({
      baseItemId: unspentBaseItem.id,
      newRelatedProductList: prevRelatedProductList
    });
  }
  // newlyの処理
  for (const spentBaseItem of spentBaseItemList) {
    const isAlreadySpent = spentBaseItem.obtainedBaseItem.related_product_list.some(product => product.id === productId);
    if (!isAlreadySpent) {// 新たな商品の場合
      const relatedProductList = spentBaseItem.obtainedBaseItem.related_product_list;
      relatedProductList.push({
        id: productId,
        name: productName,
        amount: spentBaseItem.baseItem.amount,
        measure_unit: spentBaseItem.baseItem.measure_unit,
        is_active: spentBaseItem.baseItem.is_required // is_requiredが最新情報
      });
      relatedProductListToBeUpdateList.push({
        baseItemId: spentBaseItem.baseItem.id,
        newRelatedProductList: relatedProductList
      });
      continue;
    }
    const relatedProductList = spentBaseItem.obtainedBaseItem.related_product_list;
    const activeProductIndex = relatedProductList.findIndex(product => product.id === productId);
    if (activeProductIndex > -1 && spentBaseItem.baseItem.is_active) {// 変更の場合
      relatedProductList[activeProductIndex] = {
        id: productId,
        name: productName,
        amount: spentBaseItem.baseItem.amount,
        measure_unit: spentBaseItem.baseItem.measure_unit,
        is_active: spentBaseItem.baseItem.is_required
      };
      relatedProductListToBeUpdateList.push({
        baseItemId: spentBaseItem.baseItem.id,
        newRelatedProductList: relatedProductList
      });
    }
  }
  // 更新
  for (const relatedProductList of relatedProductListToBeUpdateList) {
    for (const product of relatedProductList.newRelatedProductList) {
      removeEmptyString(product);
    }
    const params = {
      TableName: baseItemTableName,
      Key: {
        'id': relatedProductList.baseItemId
      },
      ExpressionAttributeNames: {
        "#relatedProductList": "related_product_list"
      },
      ExpressionAttributeValues: {
        ":relatedProductList": relatedProductList.newRelatedProductList
      },
      UpdateExpression: "SET #relatedProductList = :relatedProductList"
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