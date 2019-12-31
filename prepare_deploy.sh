#!/bin/sh

cd BaseItemRegisterer
zip Deploy.zip index.js
mkdir -p ../_Deploy/${DIRECTORY_NAME}/BaseItemRegisterer
mv Deploy.zip ../_Deploy/${DIRECTORY_NAME}/BaseItemRegisterer/Deploy.zip

cd ../BusinessDateHandler
zip Deploy.zip index.js
mkdir -p ../_Deploy/${DIRECTORY_NAME}/BusinessDateHandler
mv Deploy.zip ../_Deploy/${DIRECTORY_NAME}/BusinessDateHandler/Deploy.zip

cd ../FlowHandler
zip Deploy.zip index.js
mkdir -p ../_Deploy/${DIRECTORY_NAME}/FlowHandler
mv Deploy.zip ../_Deploy/${DIRECTORY_NAME}/FlowHandler/Deploy.zip

cd ../FoodRetriever
zip Deploy.zip index.js
mkdir -p ../_Deploy/${DIRECTORY_NAME}/FoodRetriever
mv Deploy.zip ../_Deploy/${DIRECTORY_NAME}/FoodRetriever/Deploy.zip

cd ../IngredientRegisterer
zip Deploy.zip index.js
mkdir -p ../_Deploy/${DIRECTORY_NAME}/IngredientRegisterer
mv Deploy.zip ../_Deploy/${DIRECTORY_NAME}/IngredientRegisterer/Deploy.zip

cd ../MaterialRegisterer
zip Deploy.zip index.js
mkdir -p ../_Deploy/${DIRECTORY_NAME}/MaterialRegisterer
mv Deploy.zip ../_Deploy/${DIRECTORY_NAME}/MaterialRegisterer/Deploy.zip

cd ../ProductRegisterer
zip Deploy.zip index.js
mkdir -p ../_Deploy/${DIRECTORY_NAME}/ProductRegisterer
mv Deploy.zip ../_Deploy/${DIRECTORY_NAME}/ProductRegisterer/Deploy.zip

cd ../ShopHandler
zip Deploy.zip index.js
mkdir -p ../_Deploy/${DIRECTORY_NAME}/ShopHandler
mv Deploy.zip ../_Deploy/${DIRECTORY_NAME}/ShopHandler/Deploy.zip

cd ../StockHandler
zip Deploy.zip index.js
mkdir -p ../_Deploy/${DIRECTORY_NAME}/StockHandler
mv Deploy.zip ../_Deploy/${DIRECTORY_NAME}/StockHandler/Deploy.zip

cd ../WholesalerRegisterer
zip Deploy.zip index.js
mkdir -p ../_Deploy/${DIRECTORY_NAME}/WholesalerRegisterer
mv Deploy.zip ../_Deploy/${DIRECTORY_NAME}/WholesalerRegisterer/Deploy.zip