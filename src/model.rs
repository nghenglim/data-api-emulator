use serde::ser::Serializer;
use serde::ser::Serialize;
// use serde::de::Deserializer;
// use serde::de::Deserialize;
use std::fmt::{Display, Formatter, Result as FmtResult};
use serde_json::{json, to_string_pretty};
use actix_web::{
    HttpResponse, ResponseError,
};
use actix_web::http::{StatusCode};
use actix_web::error::PayloadError;
use mysql::error::{Error as MysqlError};
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
pub struct BeginTransactionResponse {
    #[serde(rename="transactionId")]
    pub transaction_id: String,
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
#[derive(Debug)]
pub enum TransactionStatus {
    TransactionCommitted, // 'Transaction Committed'
    RollbackComplete, // 'Rollback Complete'
}

impl Serialize for TransactionStatus {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match *self {
            TransactionStatus::TransactionCommitted => serializer.serialize_str("Transaction Committed"),
            TransactionStatus::RollbackComplete => serializer.serialize_str("Rollback Complete"),
        }
    }
}
#[derive(Debug, Serialize)]
pub struct CommitTransactionResponse {
    #[serde(rename="transactionStatus")]
    pub transaction_status: TransactionStatus,
}
#[derive(Debug, Serialize, Deserialize)]
pub struct RollbackTransactionRequest {
    #[serde(rename="resourceArn")]
    pub resource_arn: String,
    #[serde(rename="secretArn")]
    pub secret_arn: String,
    #[serde(rename="transactionId")]
    pub transaction_id: String,
}
#[derive(Debug, Serialize)]
pub struct RollbackTransactionResponse {
    #[serde(rename="transactionStatus")]
    pub transaction_status: TransactionStatus,
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

#[derive(Debug, Serialize)]
pub struct ExecuteStatementResponse {
    #[serde(rename="numberOfRecordsUpdated")]
    pub number_of_records_updated: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename="generatedFields")]
    pub generated_fields: Option<Vec<Field>>,
    pub records: Option<Vec<Vec<Field>>>,
    #[serde(rename="columnMetadata")]
    pub column_metadata: Option<Vec<ColumnMetadata>>,
}


#[derive(Debug, Serialize, Deserialize)]
pub struct BatchExecuteStatementRequest {
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
    #[serde(rename="parameterSets")]
    pub parameter_sets: Option<Vec<Vec<SqlParameter>>>,
    #[serde(rename="transactionId")]
    pub transaction_id: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct UpdateResult {
    #[serde(rename="generatedFields")]
    pub generated_fields: Vec<Field>,
}

#[derive(Debug, Serialize)]
pub struct BatchExecuteStatementResponse {
    #[serde(rename="updateResults")]
    pub update_results: Vec<UpdateResult>,
}

#[derive(Debug, Serialize)]
pub struct ColumnMetadata {
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename="arrayBaseColumnType")]
    pub array_base_column_type: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename="isAutoIncrement")]
    pub is_auto_increment: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename="isCaseSensitive")]
    pub is_case_sensitive: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename="isCurrency")]
    pub is_currency: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename="isSigned")]
    pub is_signed: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename="label")]
    pub label: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename="name")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename="nullable")]
    pub nullable: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename="precision")]
    pub precision: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename="scale")]
    pub scale: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename="schemaName")]
    pub schema_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename="tableName")]
    pub table_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename="type")]
    pub type_: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename="typeName")]
    pub type_name: Option<String>,
}
#[derive(Debug, Serialize, Deserialize)]
pub struct SqlParameter {
    pub name: String,
    pub value: Field,
}

#[derive(Debug, Serialize)]
pub struct Error {
    pub msg: String,
    pub status: u16,
}

impl From<mysql::error::Error> for Error {
    fn from(error: MysqlError) -> Self {
        let therr: Error = match error {
            MysqlError::IoError(_err) => {
                Error {
                    msg: "Mysql IoErr".to_owned(),
                    status: 500,
                }
            }
            MysqlError::CodecError(_err) => {
                Error {
                    msg: "Mysql CodecError".to_owned(),
                    status: 500,
                }
            }
            MysqlError::MySqlError(err) => {
                Error {
                    msg: err.message.clone(),
                    status: 400,
                }
            }
            MysqlError::DriverError(_err) => {
                Error {
                    msg: "Mysql DriverError".to_owned(),
                    status: 500,
                }
            }
            MysqlError::UrlError(_err) => {
                Error {
                    msg: "Mysql UrlError".to_owned(),
                    status: 500,
                }
            }
            MysqlError::TlsError(_err) => {
                Error {
                    msg: "Mysql TlsError".to_owned(),
                    status: 500,
                }
            }
            MysqlError::TlsHandshakeError(_err) => {
                Error {
                    msg: "Mysql TlsHandshakeError".to_owned(),
                    status: 500,
                }
            }
            MysqlError::FromValueError(_err) => {
                Error {
                    msg: "Mysql FromValueError".to_owned(),
                    status: 500,
                }
            }
            MysqlError::FromRowError(_err) => {
                Error {
                    msg: "Mysql FromRowError".to_owned(),
                    status: 500,
                }
            }
        };
        therr
    }
}
impl Display for Error {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        write!(f, "{}", to_string_pretty(self).unwrap())
    }
}

impl ResponseError for Error {
    // builds the actual response to send back when an error occurs
    fn render_response(&self) -> HttpResponse {
        let err_json = json!({ "error": self.msg });
        HttpResponse::build(StatusCode::from_u16(self.status).unwrap()).json(err_json)
    }
}

impl From<PayloadError> for Error {
    fn from(error: PayloadError) -> Self {
        match error {
            _ => {
                Error {
                    msg: String::from("Payload Error"),
                    status: 500,
                }
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ColumnField {
    BlobValue,
    BooleanValue,
    DoubleValue,
    IsNull,
    LongValue,
    StringValue,
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

#[derive(Debug, Clone)]
pub struct MappedMysqlColumnType {
    pub type_name: String,
    pub column_field: ColumnField,
}
