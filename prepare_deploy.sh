#!/bin/sh

cd BaseItemRegisterer
zip -r Deploy.zip ./
mkdir -p ../_Deploy/${DIRECTORY_NAME}/BaseItemRegisterer
mv Deploy.zip ../_Deploy/${DIRECTORY_NAME}/BaseItemRegisterer/Deploy.zip

cd ../BusinessDateHandler
zip -r Deploy.zip ./
mkdir -p ../_Deploy/${DIRECTORY_NAME}/BusinessDateHandler
mv Deploy.zip ../_Deploy/${DIRECTORY_NAME}/BusinessDateHandler/Deploy.zip

cd ../FlowHandler
zip -r Deploy.zip ./
mkdir -p ../_Deploy/${DIRECTORY_NAME}/FlowHandler
mv Deploy.zip ../_Deploy/${DIRECTORY_NAME}/FlowHandler/Deploy.zip

cd ../FoodRetriever
zip -r Deploy.zip ./
mkdir -p ../_Deploy/${DIRECTORY_NAME}/FoodRetriever
mv Deploy.zip ../_Deploy/${DIRECTORY_NAME}/FoodRetriever/Deploy.zip

cd ../IngredientRegisterer
zip -r Deploy.zip ./
mkdir -p ../_Deploy/${DIRECTORY_NAME}/IngredientRegisterer
mv Deploy.zip ../_Deploy/${DIRECTORY_NAME}/IngredientRegisterer/Deploy.zip

cd ../MaterialRegisterer
zip -r Deploy.zip ./
mkdir -p ../_Deploy/${DIRECTORY_NAME}/MaterialRegisterer
mv Deploy.zip ../_Deploy/${DIRECTORY_NAME}/MaterialRegisterer/Deploy.zip

cd ../ProductRegisterer
zip -r Deploy.zip ./
mkdir -p ../_Deploy/${DIRECTORY_NAME}/ProductRegisterer
mv Deploy.zip ../_Deploy/${DIRECTORY_NAME}/ProductRegisterer/Deploy.zip

cd ../ShopHandler
zip -r Deploy.zip ./
mkdir -p ../_Deploy/${DIRECTORY_NAME}/ShopHandler
mv Deploy.zip ../_Deploy/${DIRECTORY_NAME}/ShopHandler/Deploy.zip

cd ../StockHandler
zip -r Deploy.zip ./
mkdir -p ../_Deploy/${DIRECTORY_NAME}/StockHandler
mv Deploy.zip ../_Deploy/${DIRECTORY_NAME}/StockHandler/Deploy.zip

cd ../WholesalerRegisterer
zip -r Deploy.zip ./
mkdir -p ../_Deploy/${DIRECTORY_NAME}/WholesalerRegisterer
mv Deploy.zip ../_Deploy/${DIRECTORY_NAME}/WholesalerRegisterer/Deploy.zip