## 新規Lambda作成方法
1. `pkg-ftf-lambda`直下にLambda名のディレクトリを作成し、その直下に`index.js`ファイルを作成する。
2. `pkg-ftf-lambda/prepare_deploy.sh`に新規追加のLambdaを追加
3. 以下の「S3へのアップロード方法」を参考に新規Lambdaを含むデプロイパッケージ群をS3にアップロードする。
4. `pkg-ftf-aws/CFn/04.api_gateway`下の`1.lambda.yml`と`api-gateway.yml`に新規Lambdaを追加する。
5. AWSマネジメントコンソールからCloudFormationにアクセスし、`ftf-web-management-lambda-{環境名}`のスタックを更新する。
6. Lambdaのスタック更新を確認でき次第、`ftf-web-management-api-gateway-{環境名}`のスタックを更新する。
7. API Gatewayマネジメントコンソールで更新対象のAPIを選択し、APIのデプロイを実行する。

## S3へのアップロード方法
1. `pkg-ftf-lamnbda`直下にて以下コマンドを実行する。  
Lambdaにコードの変更を反映させる場合はオブジェクトキーを前回のデプロイ時と異なるものにする必要があるが、DIRECTORY_NAMEを変更することによりオブジェクトキーの変更を実現できる。

```shell
DIRECTORY_NAME={S3にアップロードするディレクトリ名} zsh prepare_deploy.sh
```

2. `_Deploy`直下に指定した名称のディレクトリができ、各Lambdaのコードのzipが配置される。

3. AWSマネジメントコンソールでS3を開き、`ftf-web-management-lambda`バケットを選択する。

4. 作成した日付のディレクトリをD&Dでアップロードする。

## Lambdaのデプロイ方法
