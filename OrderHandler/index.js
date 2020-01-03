const AWS = require('aws-sdk');
const BigNumber = require('bignumber.js');
const docClient = new AWS.DynamoDB.DocumentClient({
  region: 'ap-northeast-1'
});

const envSuffix = process.env['ENVIRONMENT'];

const wholesalerTableName = 'WHOLESALER_' + envSuffix;
const businessDateTableName = 'BUSINESS_DATE_' + envSuffix;
const flowTableName = 'FLOW_' + envSuffix;
const foodTableName = 'FOOD_' + envSuffix;
const stockTableName = 'STOCK_' + envSuffix;
const orderTableName = 'ORDER_' + envSuffix;

exports.handler = async (event, context) => {
  
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
  await handleOrderOperation(event, response);
  return response;
}

async function handleOrderOperation(event, response) {
  switch (event.operation) {
    case 'create':
      await createOrder(event.payload, event.shopName);
      break;
    case 'retrieve-target-business-date':
      await retrieveTargetBusinessDate(event.payload, event.shopName, response);
      break;
  }
}

async function createOrder(payload, shopName) {
  // 発注対象の年月日と発注先を特定する。
  const targetWholesalerMap = await determineTargetWholesaler(payload.date, shopName)
  // 提供数平均算出
  const averageSalesList = await calcAverageSales(targetWholesalerMap, shopName);
  // 消費量平均算出
  const averageConsumptionList = await calcAverageConsumption(averageSalesList, shopName);
  console.log(JSON.stringify(averageConsumptionList))
  // 相対値をclientから渡す実装とするため、売上平均算出は行わない。
  // // 売上平均算出
  // const averageSalesPrice = await calcAverageSalesPrice(averageSalesList);
  // 係数は曜日ではなく営業日ごとにかけるのでこの操作は不要
  // 係数かける
  // const adjustedAverageConsumptionList = adjusteAverageConsumption(averageConsumptionList, payload.sales_factor);
  // console.log(JSON.stringify(adjustedAverageConsumptionList))
  // 発注量算出
  const { targetDateMap, suggestedOrderList} = await calcOrderAmount(targetWholesalerMap, averageConsumptionList, shopName, payload.sales_factor_map);
  console.log(JSON.stringify(averageConsumptionList))

  // return order;
  await putOrder({
    shop_name: shopName,
    date: payload.date,
    suggested_order_list: suggestedOrderList,
    sales_factor: payload.sales_factor,
    target_wholesaler_map: targetWholesalerMap,
    average_sales_list: averageSalesList,
    average_consumption_list: averageConsumptionList,
    target_date_Map: targetDateMap,
    execution_date: new Date().toISOString()
  })
}

async function determineTargetWholesaler(date, shopName) {
  let wholesalerMap = {};
  const wholesalerList = await retrieveWholesaler(shopName);
  const maybeTargetBusinessDateList = 
    await retrieveMaybeTargetBusinessDate(convertISO8601ToYYYYMMDD(date), shopName);

  for (const wholesaler of wholesalerList) {
    pushTargetWholesaler(wholesaler, maybeTargetBusinessDateList, wholesalerMap)
  }
  return wholesalerMap;
}

/**
 * 店舗を指定して全発注先を取得する。
 * 
 * @param {*} shopName 
 */
async function retrieveWholesaler(shopName) {
  const params = {
    TableName: wholesalerTableName,
    KeyConditionExpression: '#shopName = :shopName',
    ExpressionAttributeNames: {
      "#shopName": "shop_name"
    },
    ExpressionAttributeValues: {
      ":shopName": shopName
    }
  };
  try {
    const result = await docClient.query(params).promise();
    return result.Items;
  }
  catch (error) {
    throw error;
  }
}

/**
 * 今日以降の日付の全営業日を取得する。
 * 
 * @param {*} shopName 
 */
async function retrieveMaybeTargetBusinessDate(yyyymmdd, shopName) {
  const params = {
    TableName: businessDateTableName,
    KeyConditionExpression: "#shopName = :shopName and #date > :date",
    ExpressionAttributeNames:{
      "#shopName": "shop_name",
      "#date": "date"
    },
    ExpressionAttributeValues: {
      ":shopName": shopName,
      ":date": yyyymmdd
    },
    Limit: 10
  };
  try {
    const result = await docClient.query(params).promise();
    return result.Items
  }
  catch (error) {
    throw error;
  }
}

function pushTargetWholesaler(wholesaler, maybeTargetBusinessDateList, wholesalerMap) {
  const nextDateIso8601 = convertYYYYMMDDToISO8601(maybeTargetBusinessDateList[0].date);
  // 翌日が休みなら発注しない
  if (maybeTargetBusinessDateList[0].closed_wholesaler_list.some(closed => closed.id === wholesaler.id)) {
    return;
  }
  // ##############
  //  以降、発注がある場合
  // ##############
  if (!wholesalerMap[nextDateIso8601]) {// 最初の発注先
    wholesalerMap[nextDateIso8601] = {};
    wholesalerMap[nextDateIso8601]["targetWholesalerList"] = [{
      id: wholesaler.id,
      name: wholesaler.name
    }];
    wholesalerMap[nextDateIso8601]['day'] = getDayFromISO8601(nextDateIso8601);
  } else { // ２回目以降
    wholesalerMap[nextDateIso8601]["targetWholesalerList"].push({
      id: wholesaler.id,
      name: wholesaler.name
    });
  }
  // ##############
  // 翌々日以降のループ
  // ##############
  for (let index = 1; index < maybeTargetBusinessDateList.length; index++) {
    const nextDateIso8601 = convertYYYYMMDDToISO8601(maybeTargetBusinessDateList[index].date);
    // 翌々日が休みなら発注対象
    if (maybeTargetBusinessDateList[index].closed_wholesaler_list.some(closed => closed.id === wholesaler.id)) {
      if (!wholesalerMap[nextDateIso8601]) {// 最初の発注先
        wholesalerMap[nextDateIso8601] = {};
        wholesalerMap[nextDateIso8601]["targetWholesalerList"] = [{
          id: wholesaler.id,
          name: wholesaler.name
        }];
        wholesalerMap[nextDateIso8601]['day'] = getDayFromISO8601(nextDateIso8601);
      } else { // ２回目以降
        wholesalerMap[nextDateIso8601]["targetWholesalerList"].push({
          id: wholesaler.id,
          name: wholesaler.name
        });
      }
    } else {
      break;
    }
  }
}

async function calcAverageSales(targetWholesalerMap, shopName) {
  // 対象曜日取得
  const dayList = [];
  const keyList = Object.keys(targetWholesalerMap);
  for (const key of keyList) {
    dayList.push(targetWholesalerMap[key].day);
  }

  // 過去４回の対象曜日のProduct Flowを取得(key: 曜日、value: product flowのList)
  const productFlowMap = await retrieveProductFlow(dayList, shopName);

  const averageSalesList = [];
  for (const day of dayList) {
    averageSalesList.push({
      day: day,
      averageSalesMap: calcAverageSalesForDay(day, productFlowMap[day])
    });
  }
  return averageSalesList;
}

async function retrieveProductFlow(dayList, shopName) {
  let productFlowMap = {};

  for (const day of dayList) {
    const params = {
      TableName: flowTableName,
      KeyConditions: {
        'shop_name_food_type': {
          ComparisonOperator: 'EQ',
          AttributeValueList: [shopName + ':product']
        }
      },
      QueryFilter: {
        'day': {
          ComparisonOperator: 'EQ',
          AttributeValueList: [
            day
          ]
        }
      },
      ScanIndexForward: false,
      Limit: 4
    };
    try {
      const result = await docClient.query(params).promise();
      productFlowMap[day] = result.Items.map(item => item.flow_data);
    }
    catch (error) {
      throw error;
    }
  }

  return productFlowMap;
}

function calcAverageSalesForDay(day, productFlowList) {
  // ４週間で提供された全商品のID取得
  let productIdList = [];
  for (const flowData of productFlowList) {
    let newFlowData = {};
    for (const id of Object.keys(flowData)) {
      if (flowData[id].menu_type !== 'regular') {
        continue;
      }
      productIdList.push(id);
    }
  }
  productIdList = Array.from(new Set(productIdList));

  // 平均算出
  let averageSalesMap = {};
  for (const productId of productIdList) {
    averageSalesMap[productId] = productFlowList
      .map(productFlow => convertBigNumber(productFlow[productId].daily_amount))
      .reduce((acc, cur) => {
        return acc.plus(cur);
      }, new BigNumber(0))
      .dividedBy(new BigNumber(4))
      .toFixed(2);
  }
  return averageSalesMap;
}

async function calcAverageConsumption(averageSalesList, shopName) {
  // 各曜日の平均消費量算出
  const promises = [];
  for (const averageSales of averageSalesList) {
    promises.push(calcConsumptionForDay(averageSales, shopName));
  }
  try {
    const consumptionList = await Promise.all(promises);
    return new Promise((resolve) => resolve(consumptionList))
  }
  catch (error) {
    throw error;
  }
}

async function calcConsumptionForDay(averageSales, shopName) {
  // パラメータ取得
  const day = averageSales.day;
  const averageSalesMap = averageSales.averageSalesMap;

  // 各商品ごとに食材消費量算出し、mapにまとめる
  const productIdList = Object.keys(averageSalesMap);
  let materialMap = {}
  const promises = [];
  for (const productId of productIdList) {
    promises.push(calcMaterialForProduct(productId, averageSalesMap[productId], shopName, materialMap));
  }

  try {
    await Promise.all(promises);
    return new Promise((resolve) => {
      resolve({
        day: day,
        averageMaterialMap: materialMap
      })
    })
  }
  catch (error) {
    throw error;
  }
}

async function calcMaterialForProduct(productId, averageSales, shopName, materialMap) {
  // product    -> base-item  + ingredient
  const { baseItemList, ingredientForProductList } =
    await calcBaseItemAndIngredientFromProduct(productId, averageSales, shopName);

  // base-item  -> ingredient + material
  // ここを非同期にしようとするとなぜかmaterialMapへのpushがうまくいかないので同期的に処理する
  const ingredientFromBaseItemList =
    await calcIngredientAndMaterialFromBaseItemList(baseItemList, shopName, materialMap)
  // ingredient -> material
  await calcMaterialFromIngredientList(ingredientForProductList, shopName, materialMap)

  // (base-item ->) ingredient -> material
  await calcMaterialFromIngredientList(ingredientFromBaseItemList, shopName, materialMap);
}

async function calcBaseItemAndIngredientFromProduct(productId, averageSales, shopName) {
  const product = await findFoodByShopNameAndFoodTypeAndId(shopName, 'product', productId);
  return {
    baseItemList:
      product.required_base_item_list
        .filter(baseItem => baseItem.is_active)
        .map(baseItem => {
          return {
            id: baseItem.id,
            name: baseItem.name,
            amount: convertBigNumber(baseItem.amount).times(convertBigNumber(averageSales)).toFixed(2)
          }
        }),
    ingredientForProductList:
      product.required_ingredient_list
        .filter(ingredient => ingredient.is_active)
        .map(ingredient => {
          return {
            id: ingredient.id,
            name: ingredient.name,
            amount: convertBigNumber(ingredient.amount).times(convertBigNumber(averageSales)).toFixed(2)
          }
        })
  };
}

async function calcIngredientAndMaterialFromBaseItemList(baseItemList, shopName, materialMap) {
  const ingredientList = [];
  const promises = [];
  for (const baseItem of baseItemList) {
    promises.push(calcIngredientAndMaterialFromBaseItem(baseItem, shopName, materialMap, ingredientList));
  }

  try {
    const results = await Promise.all(promises);
    return new Promise((resolve) => resolve(results[0] ? results[0] : []));
  }
  catch (error) {
    throw error;
  }
}

async function calcIngredientAndMaterialFromBaseItem(baseItem, shopName, materialMap, ingredientList) {
  const baseItemRaw = await findFoodByShopNameAndFoodTypeAndId(shopName, 'base-item', baseItem.id);

  const requiredIngredientList = baseItemRaw.required_ingredient_list;
  const requiredMaterialList = baseItemRaw.required_material_list;
  const measurePerPrepare = convertBigNumber(baseItemRaw.measure.measure_per_prepare);

  if (measurePerPrepare.isEqualTo(convertBigNumber(0))) throw new Error(`base item (id = ${baseItem.id}) has no mesure per prepare. please try again after registration`)

  for (const ingredient of requiredIngredientList) {
    if (!ingredient.is_active) continue;
    if (!ingredientList.some(target => target.id !== ingredient.id)) {
      ingredientList.push({
        id: ingredient.id,
        name: ingredient.name,
        amount: convertBigNumber(ingredient.amount).times(convertBigNumber(baseItem.amount)).dividedBy(measurePerPrepare).toFixed(2)
      });
    } else {
      const targetIndex = ingredientList.findIndex(target => target.id === ingredient.id)
      ingredientList[targetIndex].amount = convertBigNumber(ingredientList[targetIndex].amount).plus(convertBigNumber(ingredient.amount).times(convertBigNumber(baseItem.amount)).dividedBy(measurePerPrepare)).toFixed(2)
    }
    
  }
  for (const material of requiredMaterialList){
    if (!material.is_active) continue;
    if (!materialMap[material.id]) {
      materialMap[material.id] = convertBigNumber(material.amount).times(convertBigNumber(baseItem.amount)).dividedBy(measurePerPrepare).toFixed(2);
    } else {
      materialMap[material.id] = convertBigNumber(materialMap[material.id]).plus(convertBigNumber(material.amount).times(convertBigNumber(baseItem.amount)).dividedBy(measurePerPrepare)).toFixed(2);
    }
  }
  return new Promise((resolve) => resolve(ingredientList));
}

async function calcMaterialFromIngredientList(ingredientForProductList, shopName, materialMap) {
  const promises = [];
  for (const ingredient of ingredientForProductList) {
    promises.push(calcMaterialFromIngredient(ingredient, shopName, materialMap));
  }

  try {
    await Promise.all(promises);
    return new Promise((resolve) => resolve());
  }
  catch (error) {
    throw error;
  }
}

async function calcMaterialFromIngredient(ingredient, shopName, materialMap) {
  const ingredientRaw = await findFoodByShopNameAndFoodTypeAndId(shopName, 'ingredient', ingredient.id);

  const requiredMaterialList = ingredientRaw.required_material_list;
  const measurePerPrepare = convertBigNumber(ingredientRaw.measure.measure_per_prepare);

  if (measurePerPrepare.isEqualTo(convertBigNumber(0))) throw new Error(`ingredient (id = ${ingredient.id}) has no mesure per prepare. please try again after registration`)

  for (const material of requiredMaterialList){
    if (!material.is_active) continue;
    if (!materialMap[material.id]) {
      materialMap[material.id] = convertBigNumber(material.amount).times(convertBigNumber(ingredient.amount)).dividedBy(measurePerPrepare).toFixed(2);
    } else {
      materialMap[material.id] = convertBigNumber(materialMap[material.id]).plus(convertBigNumber(material.amount).times(convertBigNumber(ingredient.amount)).dividedBy(measurePerPrepare)).toFixed(2);
    }
  }
  return new Promise((resolve) => resolve());
}

function adjusteAverageConsumption(averageConsumptionList, salesFactor) {
  return averageConsumptionList
    .map(averageConsumption => {
      const idList = Object.keys(averageConsumption.averageMaterialMap);
      for (const id of idList) {
        averageConsumption['averageMaterialMap'][id] = convertBigNumber(averageConsumption['averageMaterialMap'][id]).times(convertBigNumber(salesFactor)).toFixed(2);
      }
      return averageConsumption;
    })
}

async function findFoodByShopNameAndFoodTypeAndId(shopName, foodType, id) {
  const params = {
    TableName: foodTableName,
    Key: {
      'shop_name_food_type': shopName + ':' + foodType,
      'id': Number(id)
    },
    ConsistentRead: true
  };

  try {
    const result = await docClient.get(params).promise();
    return result.Item;
  }
  catch (error) {
    throw error;
  }
}

async function findStockByShopNameAndFoodTypeAndId(shopName, foodType, id) {
  const params = {
    TableName: stockTableName,
    Key: {
      'shop_name_food_type': shopName + ':' + foodType,
      'id': Number(id)
    },
    ConsistentRead: true
  };

  try {
    const result = await docClient.get(params).promise();
    return result.Item;
  }
  catch (error) {
    throw error;
  }
}

async function calcOrderAmount(targetWholesalerMap, averageConsumptionList, shopName, salesFactorMap) {
  // 発注先idがKey、対象日がValueのMapを取得
  const targetDateMap = createTargetDateMap(targetWholesalerMap);
  // 発注対象の全食材のidのList取得
  const materialIdList =
    averageConsumptionList
      .map(aac => {
        return Object.keys(aac.averageMaterialMap);
      })
      .reduce((acc, cur) => {
        return acc.concat(cur);
      }, []);
  
  const promises = [];
  // 各発注対象の食材に対して
  for (const materialId of materialIdList) {
    promises.push((async () => {
      // 食材データ取得
      const material = await findFoodByShopNameAndFoodTypeAndId(shopName, 'material', materialId);  
      // 在庫残数データ取得
      const stock = await findStockByShopNameAndFoodTypeAndId(shopName, 'material', materialId);
      // 何日分発注すべきか算出
      // 対象日分の必要量算出
      const orderInfo = calcOrderInfo(targetDateMap[material.wholesaler_id], averageConsumptionList, material, stock, salesFactorMap);
      // push
      return new Promise((resolve) => {
        resolve(orderInfo)
      })
    })());
  }

  try {
     let suggestedOrderList = await Promise.all(promises);
     suggestedOrderList = suggestedOrderList.filter(so => so);
     return new Promise((resolve) => resolve({targetDateMap, suggestedOrderList}));
  }
  catch (error) {
    throw error;
  }
}

function createTargetDateMap(targetWholesalerMap) {
  const dateList = Object.keys(targetWholesalerMap);
  const wholesalerIdList =
    targetWholesalerMap[dateList[0]]["targetWholesalerList"]
    .map(wholesaler => wholesaler.id);
  
  let targetDateMap = {};
  for (const id of wholesalerIdList) {
    targetDateMap[id] = [];
    for (const date of dateList) {
      if (targetWholesalerMap[date]["targetWholesalerList"].some(wholesaler => wholesaler.id === id)) {
        targetDateMap[id].push({
          date: date,
          day: targetWholesalerMap[date].day
        })
      } else {
        break;
      }
    }
  }
  return targetDateMap;
}

function calcOrderInfo(targetDateList, averageConsumptionList, material, stock, salesFactorMap) {
  if (!targetDateList) return;
  const estimatedAmount = 
    !targetDateList
      ? 0
      : targetDateList
          .map(date => {
            return convertBigNumber(
              averageConsumptionList
                .find(aac => aac.day === date.day)['averageMaterialMap'][material.id]
              )
              .times(convertBigNumber(salesFactorMap[date.date]));
          })
          .reduce((acc, cur) => {
            return acc.plus(cur);
          }, new BigNumber(0))
          .toFixed(2);
  const measureAmount = calcMeasureAmountToBeOrdered(estimatedAmount, stock, material);
  if (measureAmount === '0.00' || measureAmount === '0') {
    return {
      id: material.id,
      name: material.name,
      measure: {
        estimated_amount: estimatedAmount,
        measure_amount: measureAmount,
        measure_unit: material.measure.measure_unit,
        stock_amount: stock.measure.measure_per_order,
        minimum_amount: material.minimum.minimum_amount
      },
      order: {
        order_amount: '0.00',
        order_unit: material.order.order_unit
      },
      needs_order: false
    };
  }
  const orderAmountRate =
    // 必要比率を小数で算出
    (convertBigNumber(measureAmount).dividedBy(convertBigNumber(material.measure.measure_per_order)))
      // modで端数を算出し、引く
      .minus(convertBigNumber(measureAmount).dividedBy(convertBigNumber(material.measure.measure_per_order)).mod(1))
      // 1繰り上げ
      .plus(1)
      .toFixed(4);
  const orderAmount =
    !targetDateList
      ? 0
      : convertBigNumber(orderAmountRate).times(convertBigNumber(material.order.amount_per_order)).toFixed(2);
  return {
    id: material.id,
    name: material.name,
    measure: {
      estimated_amount: estimatedAmount,
      measure_amount: measureAmount,
      measure_unit: material.measure.measure_unit,
      stock_amount: stock.measure.measure_per_order,
      minimum_amount: material.minimum.minimum_amount
    },
    order: {
      order_amount: orderAmount,
      order_unit: material.order.order_unit
    },
    needs_order: true
  };
}

function calcMeasureAmountToBeOrdered(estimatedAmount, stock, material) {
  const estimatedAmountBN = convertBigNumber(estimatedAmount);
  const stockAmountBN = convertBigNumber(stock.measure.measure_per_order);
  const minimumAmountBN = convertMinimumAmountToMeasure(material);

  if (stockAmountBN.minus(estimatedAmountBN).isGreaterThan(minimumAmountBN)) {
    return '0.00'; 
  } else {
    return minimumAmountBN.plus(estimatedAmountBN).minus(stockAmountBN).toFixed(2);
  }
}

function convertMinimumAmountToMeasure(material) {
  const stockMinimumUnit = material.minimum.minimum_amount_unit;
  // 最低ストックが登録されていない（＝最低ストックの単位が未登録）の場合は0を返す
  if (!stockMinimumUnit) return convertBigNumber(0);

  if (material.measure.measure_unit === stockMinimumUnit) {
    return convertBigNumber(material.minimum.minimum_amount);
  } else if (material.order.order_unit === stockMinimumUnit) {
    return convertBigNumber(material.measure.measure_per_order)
      .times(convertBigNumber(material.minimum.minimum_amount))
      .dividedBy(convertBigNumber(material.order.amount_per_order));
  } else if (material.count.count_unit === stockMinimumUnit) {
    return convertBigNumber(material.measure.measure_per_order)
      .times(convertBigNumber(material.minimum.minimum_amount))
      .dividedBy(convertBigNumber(material.count.count_per_order));
  }

}

async function putOrder(item) {
  const params = {
    TableName: orderTableName,
    Item: item
  };

  try {
    await docClient.put(params).promise();
    console.log(`[SUCCESS] registered order: ${JSON.stringify(item)}`)
  }
  catch (error) {
    throw error;
  }
}

async function retrieveTargetBusinessDate(payload, shopName, response) {
  // 発注対象の年月日と発注先を特定する。
  const targetWholesalerMap = await determineTargetWholesaler(payload.date, shopName)
  // 発注先idがKey、対象日がValueのMapを取得
  const targetDateMap = createTargetDateMap(targetWholesalerMap);
  console.log(JSON.stringify(targetWholesalerMap));
  console.log(JSON.stringify(targetDateMap));
  response.body.targetWholesalerMap = targetWholesalerMap;
  response.body.targetDateMap = targetDateMap;
  await putOrder({
    shop_name: shopName,
    date: payload.date,
    target_wholesaler_map: targetWholesalerMap,
    target_date_map: targetDateMap
  })
}

function getDayFromISO8601(date) {
  const dateUTC = new Date(date);
  return new Date(dateUTC.setHours(dateUTC.getHours() + 9)).getDay();
}

function getDayFromYYYYMMDD(date) {
  const dateUTC = new Date(date);
  return new Date(dateUTC.setHours(dateUTC.getHours() + 9)).getDay();
}

function convertISO8601ToYYYYMMDD(dateISO8601) {
  const date = new Date(dateISO8601);
  const dateLocal = new Date(date.setHours(date.getHours() + 9));
  return dateLocal.toISOString().slice(0, 10);
}

function convertYYYYMMDDToISO8601(yyyymmdd) {
  const date = new Date(yyyymmdd);
  const dateLocal = new Date(date.setHours(date.getHours() - 9));
  return dateLocal.toISOString();
}

function convertBigNumber(object) {
  return object ? new BigNumber(object) : new BigNumber(0);
}

function convertNum(object) {
  return object ? Number(object) : 0;
}