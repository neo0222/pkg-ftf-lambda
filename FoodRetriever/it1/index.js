const AWS = require('aws-sdk');
const docClient = new AWS.DynamoDB.DocumentClient({
  region: 'ap-northeast-1'
});

const envSuffix = process.env['ENVIRONMENT'];

const productTableName = 'PRODUCT_' + envSuffix;
const ingredientTableName = 'INGREDIENT_' + envSuffix;
const materialTableName = 'MATERIAL_' + envSuffix;
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

const response = {
    statusCode: 200,
    body: {
      foodList: []
    },
    headers: {
        'Access-Control-Allow-Origin': '*',
    }
};

async function main(event, context) {
  console.log(JSON.stringify(response));
  response.body.foodList.length = 0;
  try {
    await handleFoodType(event);
  }
  catch(error) {
    console.log(`[ERROR] failed to retrieve food data`);
    console.error(error);
    throw error;
  }
  console.log(JSON.stringify(response));
  return response;
}

async function handleFoodType(event) {
  switch (event.foodType) {
    case 'product':
      await handleProductOperation(event);
      break;
    case 'material':
      await handleMaterialOperation(event);
      break;
    case 'ingredient':
      await handleIngredientOperation(event);
      break;
    case 'base-item':
      await handleBaseItemOperation(event);
      break;
    case 'wholesaler':
      await handleWholesalerOperation(event);
      break;
  }
}

async function handleProductOperation(event) {
  switch (event.operation) {
    case 'findAll':
      await getProduct(productTableName, event.item);
      break;
  }
}

async function handleMaterialOperation(event) {
  // todo: implement
  switch (event.operation) {
    case 'findAll':
      console.log('invoked');
      await getMaterial(materialTableName, event.item);
      break;
    case 'find-unregistered-material':
      await getUnregisteredMaterial(event.payload);
      break;
    case 'find-by-material-type':
      await getMaterialByMaterialType(event.payload);
      break;
    case 'update':
      // await updateMaterial(materialTableName, event.item);
      break;
  }
}

async function handleIngredientOperation(event) {
  // todo: implement
  switch (event.operation) {
    case 'findAll':
      await getIngredient(ingredientTableName, event.item);
      break;
    // case 'update':
    //   await updateIngredient(ingredientTableName, event.payload);
    //   break;
  }
}

async function handleBaseItemOperation(event) {
  switch (event.operation) {
    case 'findAll':
      await getBaseItem(baseItemTableName, event.item);
      break;
  }
}

async function handleWholesalerOperation(event) {
  //todo: implement
}

async function getProduct(productTableName, item) {
  const params = {
      TableName: productTableName
  };
  try {
    const result = await docClient.scan(params).promise();
    console.log(`[SUCCESS] retrieved ${result.Count} data: `);
    for (const item of result.Items) {
      response.body.foodList.push(item);
    }
  }
  catch(error) {
    console.log(`[ERROR] failed to retrieve food data`);
    console.error(error);
    throw error;
  }
}

async function getMaterial(materialTableName, item) {
  const params = {
      TableName: materialTableName
  };
  try {
    const result = await docClient.scan(params).promise();
    console.log(`[SUCCESS] retrieved ${result.Count} data: `);
    for (const item of result.Items) {
      response.body.foodList.push({
        id: item.id,
        name: item.name,
        wholesaler_id: item.wholesaler_id,
        material_code: item.material_code,
        material_type: item.material_type,
        price_per_order: item.price_per_order,
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
        related_ingredient_list: item.related_ingredient_list,
        related_base_item_list: item.related_base_item_list,
        is_active: item.is_active,
        is_deleted: item.is_deleted
      });
    }
  }
  catch(error) {
    console.log(`[ERROR] failed to retrieve food data`);
    console.error(error);
    throw error;
  }
}

async function getIngredient(ingredientTableName, item) {
  const params = {
    TableName: ingredientTableName
  };
  try {
    const result = await docClient.scan(params).promise();
    console.log(`[SUCCESS] retrieved ${result.Count} data: ${JSON.stringify(result.Items)}`);
    for (const item of result.Items) {
      response.body.foodList.push({
        id: item.id,
        name: item.name,
        omitted_name: item.omitted_name,
        preparation_type: item.preparation_type,
        price_per_prepare: item.price_per_prepare,
        amount_per_prepare: item.prepare.amount_per_prepare,
        prepare_unit: item.prepare.prepare_unit,
        measure_per_prepare: item.measure.measure_per_prepare,
        measure_unit: item.measure.measure_unit,
        minimum_amount: item.minimum.minimum_amount,
        minimum_amount_unit: item.minimum.minimum_amount_unit,
        proper_amount: item.proper.proper_amount,
        proper_amount_unit: item.proper.proper_amount_unit,
        required_material_list: item.required_material_list,
        related_product_list: item.related_product_list,
        related_base_item_list: item.related_base_item_list,
        related_material: item.related_material,
        is_active: item.is_active,
        is_deleted: item.is_deleted
      });
    }
  }
  catch (error) {
    throw error;
  }
}

async function getBaseItem(baseItemTableName, item) {
  const params = {
    TableName: baseItemTableName
  };
  try {
    const result = await docClient.scan(params).promise();
    console.log(`[SUCCESS] retrieved ${result.Count} base item data: ${JSON.stringify(result.Items)}`);
    for (const item of result.Items) {
      response.body.foodList.push({
        id: item.id,
        name: item.name,
        omitted_name: item.omitted_name,
        menu_type: item.menu_type,
        product_type_1: item.product_type_1,
        product_type_2: item.product_type_2,
        price_per_prepare: item.price_per_prepare,
        amount_per_prepare: item.prepare.amount_per_prepare,
        prepare_unit: item.prepare.prepare_unit,
        measure_per_prepare: item.measure.measure_per_prepare,
        measure_unit: item.measure.measure_unit,
        minimum_amount: item.minimum.minimum_amount,
        minimum_amount_unit: item.minimum.minimum_amount_unit,
        proper_amount: item.proper.proper_amount,
        proper_amount_unit: item.proper.proper_amount_unit,
        required_material_list: item.required_material_list,
        required_ingredient_list: item.required_ingredient_list,
        related_product_list: item.related_product_list,
        is_active: item.is_active,
        is_deleted: item.is_deleted
      });
    }
  }
  catch (error) {
    throw error;
  }
}

async function getUnregisteredMaterial(payload) {
  const materialList = payload.materialList;
  const unregisteredMaterialCodeList = [];
  const promises = []
  for (const material of materialList) {
    promises.push((async () => {
      const params = {
        TableName: materialTableName,
        IndexName: 'material_code-index',//インデックス名を指定
        ExpressionAttributeNames:{'#m': 'material_code'},
        ExpressionAttributeValues:{':val': material.material_code},
        KeyConditionExpression: '#m = :val'//検索対象が満たすべき条件を指定
      };
      try {
        const result = await docClient.query(params).promise();
        if (result.Items.length === 0) {
          unregisteredMaterialCodeList.push(material.material_code);
        }
      }
      catch (error) {
        console.error(error);
        throw error;
      }
    })());
  }
  try {
    await Promise.all(promises);
    for (const materialCode of unregisteredMaterialCodeList) {
      response.body.foodList.push(materialCode);
    }
  }
  catch (error) {
    console.error(error);
    throw error;
  }
}

async function getMaterialByMaterialType(payload) {
  const materialType = payload.materialType;
  const params = {
    TableName: materialTableName,
    IndexName: 'material_type-index',//インデックス名を指定
    ExpressionAttributeNames:{'#m': 'material_type'},
    ExpressionAttributeValues:{':val': materialType},
    KeyConditionExpression: '#m = :val'//検索対象が満たすべき条件を指定
  };
  try {
    const result = await docClient.query(params).promise();
    for (const item of result.Items) {
      response.body.foodList.push({
        id: item.id,
        name: item.name,
        wholesaler_id: item.wholesaler_id,
        material_code: item.material_code,
        material_type: item.material_type,
        price_per_order: item.price_per_order,
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
        related_ingredient_list: item.related_ingredient_list,
        related_base_item_list: item.related_base_item_list,
        is_active: item.is_active,
        is_deleted: item.is_deleted
      });
    }
  }
  catch (error) {
    console.error(error);
    throw error;
  }
}

function putResultOnResponse(result, response, foodType) {
  if (foodType === 'product') {
    for (const item of result.Items) {
      response.body.foodList.push(item);
    }
  } else if (foodType === 'material') {
    for (const item of result.Items) {
      response.body.foodList.push({
        id: item.id,
        name: item.name,
        wholesaler_id: item.wholesaler_id,
        material_type: item.material_type,
        price_per_order: item.price_per_order,
        amount_per_order: item.order.amount_per_order,
        order_unit: item.order.order_unit,
        count_per_order: item.count.count_per_order,
        count_unit: item.count.count_unit,
        measure_per_order: item.measure.measure_per_order,
        measure_unit: item.measure.measure_unit,
        minimum_amount: item.minimum.minimum_amount,
        minimum_amount_unit: item.minimum.minimum_amount_unit,
        proper_amount: item.proper.proper_amount,
        proper_amount_unit: item.proper.proper_amount_unit
      });
    }
  }
}

