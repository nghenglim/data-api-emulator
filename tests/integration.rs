#[macro_use]
extern crate serde_derive;
extern crate reqwest;
// use std::collections::HashMap;

#[derive(Debug, Serialize, Deserialize)]
pub struct BeginTransactionResponse {
    #[serde(rename="transactionId")]
    pub transaction_id: String,
}
#[derive(Debug, Serialize, Deserialize)]
pub struct ExecuteStatementRequest {
    #[serde(rename="resourceArn")]
    pub resource_arn: String,
    #[serde(rename="secretArn")]
    pub secret_arn: String,
    pub sql: String,
    pub schema: Option<String>,
    pub database: Option<String>,
    #[serde(rename="continueAfterTimeout")]
    pub continue_after_timeout: Option<String>,
    #[serde(rename="includeResultMetadata")]
    pub include_result_metadata: Option<bool>,
    pub parameters: Option<Vec<SqlParameter>>,
    #[serde(rename="transactionId")]
    pub transaction_id: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SqlParameter {
    pub name: String,
    pub value: Field,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BeginTransactionRequest {
    #[serde(rename="resourceArn")]
    pub resource_arn: String,
    #[serde(rename="secretArn")]
    pub secret_arn: String,
    pub schema: Option<String>,
    pub database: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CommitTransactionRequest {
    #[serde(rename="resourceArn")]
    pub resource_arn: String,
    #[serde(rename="secretArn")]
    pub secret_arn: String,
    #[serde(rename="transactionId")]
    pub transaction_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Field {
    #[serde(rename="blobValue")]
    BlobValue(String),
    #[serde(rename="booleanValue")]
    BooleanValue(bool),
    #[serde(rename="doubleValue")]
    DoubleValue(f64),
    #[serde(rename="isNull")]
    IsNull(bool),
    #[serde(rename="longValue")]
    LongValue(i64),
    #[serde(rename="stringValue")]
    StringValue(String),
}

// #[actix_rt::test]
// #[serial]
// async fn dummy_test() {
//     let body = reqwest::get("http://localhost:8080")
//         .await
//         .unwrap()
//         .text()
//         .await
//         .unwrap();

//     assert_eq!(body, "{\"stringValue\":\"ok\"}");
// }

const RESOURCE_ARN: &'static str = "arn:aws:rds:us-east-1:123456789012:cluster:dummy";
const SECRET_ARN: &'static str = "arn:aws:secretsmanager:us-east-1:123456789012:secret:dummy";
const DATABASE_TEST: &'static str = "leliam_data_api";
const DATABASE_MAIN: &'static str = "mysql";
const CREATE_TEST_TABLE_SCHEMA: &'static str = "CREATE TABLE doc(
    `key` varchar(255) NOT NULL,
    `content` longtext NULL,
    `content_extra` longtext NULL,
    `created_at` timestamp DEFAULT CURRENT_TIMESTAMP NOT NULL,
    `updated_at` timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP NOT NULL,
    PRIMARY KEY (`key`)
);";
const CREATE_TEST_TABLEB_SCHEMA: &'static str = "CREATE TABLE docb (
    `key` varchar(255) NOT NULL,
    `content` longtext NULL,
    `content_extra` longtext NULL,
    `created_at` timestamp DEFAULT CURRENT_TIMESTAMP NOT NULL,
    `updated_at` timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP NOT NULL,
    PRIMARY KEY (`key`)
);";
#[actix_rt::test]
async fn step_1_create_fresh_table() {
    let req: ExecuteStatementRequest = ExecuteStatementRequest {
        resource_arn: RESOURCE_ARN.to_owned(),
        secret_arn: SECRET_ARN.to_owned(),
        sql: format!("DROP DATABASE IF EXISTS {}", DATABASE_TEST),
        schema: None,
        database: Some(DATABASE_MAIN.to_owned()),
        continue_after_timeout: None,
        include_result_metadata: None,
        parameters: None,
        transaction_id: None,
    };

    let client = reqwest::Client::new();
    let status = client.post("http://localhost:8080/Execute")
        .json(&req)
        .send()
        .await
        .unwrap()
        .status();

    assert_eq!(status, 200);

    let req: ExecuteStatementRequest = ExecuteStatementRequest {
        resource_arn: RESOURCE_ARN.to_owned(),
        secret_arn: SECRET_ARN.to_owned(),
        sql: format!("CREATE DATABASE {}", DATABASE_TEST),
        schema: None,
        database: Some(DATABASE_MAIN.to_owned()),
        continue_after_timeout: None,
        include_result_metadata: None,
        parameters: None,
        transaction_id: None,
    };

    let body = client.post("http://localhost:8080/Execute")
        .json(&req)
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();

    assert_eq!(body, "{\"numberOfRecordsUpdated\":1,\"records\":[],\"columnMetadata\":[]}");

    let req: ExecuteStatementRequest = ExecuteStatementRequest {
        resource_arn: RESOURCE_ARN.to_owned(),
        secret_arn: SECRET_ARN.to_owned(),
        sql: CREATE_TEST_TABLE_SCHEMA.to_owned(),
        schema: None,
        database: Some(DATABASE_TEST.to_owned()),
        continue_after_timeout: None,
        include_result_metadata: None,
        parameters: None,
        transaction_id: None,
    };

    let body = client.post("http://localhost:8080/Execute")
        .json(&req)
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();

    assert_eq!(body, "{\"numberOfRecordsUpdated\":0,\"records\":[],\"columnMetadata\":[]}");

    let req: ExecuteStatementRequest = ExecuteStatementRequest {
        resource_arn: RESOURCE_ARN.to_owned(),
        secret_arn: SECRET_ARN.to_owned(),
        sql: CREATE_TEST_TABLEB_SCHEMA.to_owned(),
        schema: None,
        database: Some(DATABASE_TEST.to_owned()),
        continue_after_timeout: None,
        include_result_metadata: None,
        parameters: None,
        transaction_id: None,
    };

    let body = client.post("http://localhost:8080/Execute")
        .json(&req)
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();

    assert_eq!(body, "{\"numberOfRecordsUpdated\":0,\"records\":[],\"columnMetadata\":[]}");
}

#[actix_rt::test]
async fn step_2_perform_transaction() {
    let req = BeginTransactionRequest {
        resource_arn: RESOURCE_ARN.to_owned(),
        secret_arn: SECRET_ARN.to_owned(),
        schema: None,
        database: Some(DATABASE_TEST.to_owned()),
    };

    let client = reqwest::Client::new();
    let transaction_1: BeginTransactionResponse = client.post("http://localhost:8080/BeginTransaction")
        .json(&req)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    let transaction_2: BeginTransactionResponse = client.post("http://localhost:8080/BeginTransaction")
        .json(&req)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();


    let req: ExecuteStatementRequest = ExecuteStatementRequest {
        resource_arn: RESOURCE_ARN.to_owned(),
        secret_arn: SECRET_ARN.to_owned(),
        sql: "INSERT INTO `doc` (`key`, `content`) VALUES (:key, :contentValue) ON DUPLICATE KEY UPDATE content = :contentValue".to_owned(),
        schema: None,
        database: Some(DATABASE_TEST.to_owned()),
        continue_after_timeout: None,
        include_result_metadata: None,
        parameters: Some(vec![SqlParameter{name: "key".to_owned(), value: Field::StringValue("doc_a".to_owned())}, SqlParameter{name: "contentValue".to_owned(), value: Field::StringValue("somecontentvalue".to_owned())}]),
        transaction_id: Some(transaction_1.transaction_id.clone()),
    };
    let treq1 = client.post("http://localhost:8080/Execute")
        .json(&req)
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();

    assert_eq!(treq1, "{\"numberOfRecordsUpdated\":1,\"records\":[],\"columnMetadata\":[]}");

    let req: ExecuteStatementRequest = ExecuteStatementRequest {
        resource_arn: RESOURCE_ARN.to_owned(),
        secret_arn: SECRET_ARN.to_owned(),
        sql: "INSERT INTO `doc` (`key`, `content`, `content_extra`, `updated_at`) VALUES (:key, 'testing''the\\'content value', \"I'm the king \\\"\", '2021-01-28 01:37:51') ON DUPLICATE KEY UPDATE content = :content_value".to_owned(),
        schema: None,
        database: Some(DATABASE_TEST.to_owned()),
        continue_after_timeout: None,
        include_result_metadata: None,
        parameters: Some(vec![SqlParameter{name: "key".to_owned(), value: Field::StringValue("doc_b".to_owned())}, SqlParameter{name: "content_value".to_owned(), value: Field::StringValue("somecontentvalue".to_owned())}]),
        transaction_id: Some(transaction_2.transaction_id.clone()),
    };

    let treq2 = client.post("http://localhost:8080/Execute")
        .json(&req)
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();

    assert_eq!(treq2, "{\"numberOfRecordsUpdated\":1,\"records\":[],\"columnMetadata\":[]}");

    let req = CommitTransactionRequest {
        resource_arn: RESOURCE_ARN.to_owned(),
        secret_arn: SECRET_ARN.to_owned(),
        transaction_id: transaction_1.transaction_id.clone(),
    };

    let body = client.post("http://localhost:8080/CommitTransaction")
        .json(&req)
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert_eq!(body, "{\"transactionStatus\":\"Transaction Committed\"}");

    let req = CommitTransactionRequest {
        resource_arn: RESOURCE_ARN.to_owned(),
        secret_arn: SECRET_ARN.to_owned(),
        transaction_id: transaction_2.transaction_id.clone(),
    };

    let body = client.post("http://localhost:8080/CommitTransaction")
        .json(&req)
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert_eq!(body, "{\"transactionStatus\":\"Transaction Committed\"}");
}
