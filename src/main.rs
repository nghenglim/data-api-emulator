extern crate rand;

#[macro_use]
extern crate actix_web;

#[macro_use]
extern crate serde_derive;

mod model;

use crate::model::SqlParameter;
use dotenv::dotenv;
use std::sync::Mutex;
use crate::rand::Rng;
use std::collections::HashMap;
use std::sync::Arc;
use std::{env, io};

use mysql::{
    consts::{ColumnType as MysqlColumnType, ColumnFlags as MysqlColumnFlags},
    Value as MysqlValue,
    Params as MysqlParams,
};
use twox_hash::XxHash;
use actix_web::http::{StatusCode};
use actix_web::{
    guard, middleware, web, App, HttpRequest, HttpResponse, HttpServer,
};
use core::hash::BuildHasherDefault;
use crate::model::{TransactionStatus, ColumnField, Field, BeginTransactionRequest,BeginTransactionResponse,CommitTransactionRequest,CommitTransactionResponse,RollbackTransactionRequest,RollbackTransactionResponse,ExecuteStatementRequest,ExecuteStatementResponse,BatchExecuteStatementRequest,UpdateResult,BatchExecuteStatementResponse,ColumnMetadata,Error,MappedMysqlColumnType};
fn map_mysql_column_type(in_column_type: MysqlColumnType, in_column_flags: MysqlColumnFlags) -> MappedMysqlColumnType {
    let column_type = match in_column_type {
        MysqlColumnType::MYSQL_TYPE_DECIMAL => "DECIMAL",
        MysqlColumnType::MYSQL_TYPE_TINY => "TINY",
        MysqlColumnType::MYSQL_TYPE_SHORT => "SHORT",
        MysqlColumnType::MYSQL_TYPE_LONG => "LONG",
        MysqlColumnType::MYSQL_TYPE_FLOAT => "FLOAT",
        MysqlColumnType::MYSQL_TYPE_DOUBLE => "DOUBLE",
        MysqlColumnType::MYSQL_TYPE_NULL => "NULL",
        MysqlColumnType::MYSQL_TYPE_TIMESTAMP => "TIMESTAMP",
        MysqlColumnType::MYSQL_TYPE_LONGLONG => "LONGLONG",
        MysqlColumnType::MYSQL_TYPE_INT24 => "INT24",
        MysqlColumnType::MYSQL_TYPE_DATE => "DATE",
        MysqlColumnType::MYSQL_TYPE_TIME => "TIME",
        MysqlColumnType::MYSQL_TYPE_DATETIME => "DATETIME",
        MysqlColumnType::MYSQL_TYPE_YEAR => "YEAR",
        MysqlColumnType::MYSQL_TYPE_NEWDATE => "NEWDATE",
        MysqlColumnType::MYSQL_TYPE_VARCHAR => "VARCHAR",
        MysqlColumnType::MYSQL_TYPE_BIT => "BIT",
        MysqlColumnType::MYSQL_TYPE_TIMESTAMP2 => "TIMESTAMP2",
        MysqlColumnType::MYSQL_TYPE_DATETIME2 => "DATETIME2",
        MysqlColumnType::MYSQL_TYPE_TIME2 => "TIME2",
        MysqlColumnType::MYSQL_TYPE_JSON => "JSON",
        MysqlColumnType::MYSQL_TYPE_NEWDECIMAL => "NEWDECIMAL",
        MysqlColumnType::MYSQL_TYPE_ENUM => "ENUM",
        MysqlColumnType::MYSQL_TYPE_SET => "SET",
        MysqlColumnType::MYSQL_TYPE_TINY_BLOB => "TINY_BLOB",
        MysqlColumnType::MYSQL_TYPE_MEDIUM_BLOB => "MEDIUM_BLOB",
        MysqlColumnType::MYSQL_TYPE_LONG_BLOB => "LONG_BLOB",
        MysqlColumnType::MYSQL_TYPE_BLOB => "BLOB",
        MysqlColumnType::MYSQL_TYPE_VAR_STRING => "VAR_STRING",
        MysqlColumnType::MYSQL_TYPE_STRING => "STRING",
        MysqlColumnType::MYSQL_TYPE_GEOMETRY => "GEOMETRY",
    };
    let mut column_field = match in_column_type {
        MysqlColumnType::MYSQL_TYPE_DECIMAL => ColumnField::DoubleValue,
        MysqlColumnType::MYSQL_TYPE_TINY => ColumnField::BooleanValue,
        MysqlColumnType::MYSQL_TYPE_SHORT => ColumnField::LongValue,
        MysqlColumnType::MYSQL_TYPE_LONG => ColumnField::LongValue,
        MysqlColumnType::MYSQL_TYPE_FLOAT => ColumnField::DoubleValue,
        MysqlColumnType::MYSQL_TYPE_DOUBLE => ColumnField::DoubleValue,
        MysqlColumnType::MYSQL_TYPE_NULL => ColumnField::IsNull,
        MysqlColumnType::MYSQL_TYPE_TIMESTAMP => ColumnField::StringValue,
        MysqlColumnType::MYSQL_TYPE_LONGLONG => ColumnField::LongValue,
        MysqlColumnType::MYSQL_TYPE_INT24 => ColumnField::LongValue,
        MysqlColumnType::MYSQL_TYPE_DATE => ColumnField::StringValue,
        MysqlColumnType::MYSQL_TYPE_TIME => ColumnField::StringValue,
        MysqlColumnType::MYSQL_TYPE_DATETIME => ColumnField::StringValue,
        MysqlColumnType::MYSQL_TYPE_YEAR => ColumnField::StringValue,
        MysqlColumnType::MYSQL_TYPE_NEWDATE => ColumnField::StringValue,
        MysqlColumnType::MYSQL_TYPE_VARCHAR => ColumnField::StringValue,
        MysqlColumnType::MYSQL_TYPE_BIT => ColumnField::BooleanValue,
        MysqlColumnType::MYSQL_TYPE_TIMESTAMP2 => ColumnField::StringValue,
        MysqlColumnType::MYSQL_TYPE_DATETIME2 => ColumnField::StringValue,
        MysqlColumnType::MYSQL_TYPE_TIME2 => ColumnField::StringValue,
        MysqlColumnType::MYSQL_TYPE_JSON => ColumnField::StringValue,
        MysqlColumnType::MYSQL_TYPE_NEWDECIMAL => ColumnField::DoubleValue,
        MysqlColumnType::MYSQL_TYPE_ENUM => ColumnField::StringValue,
        MysqlColumnType::MYSQL_TYPE_SET => ColumnField::StringValue,
        MysqlColumnType::MYSQL_TYPE_TINY_BLOB => ColumnField::BlobValue,
        MysqlColumnType::MYSQL_TYPE_MEDIUM_BLOB => ColumnField::BlobValue,
        MysqlColumnType::MYSQL_TYPE_LONG_BLOB => ColumnField::BlobValue,
        MysqlColumnType::MYSQL_TYPE_BLOB => ColumnField::BlobValue,
        MysqlColumnType::MYSQL_TYPE_VAR_STRING => ColumnField::StringValue,
        MysqlColumnType::MYSQL_TYPE_STRING => ColumnField::StringValue,
        MysqlColumnType::MYSQL_TYPE_GEOMETRY => ColumnField::StringValue,
    };
    if column_field == ColumnField::BlobValue && !in_column_flags.contains(MysqlColumnFlags::BINARY_FLAG) {
        column_field = ColumnField::StringValue
    }
    MappedMysqlColumnType {
        type_name: column_type.to_string(),
        column_field: column_field,
    }
}

async fn p404() -> Result<HttpResponse, Error> {
    Ok(HttpResponse::build(StatusCode::NOT_FOUND)
        .content_type("text/html; charset=utf-8")
        .body("route not found"))
}

struct CheckArnParam {
    resource_arn: String,
    secret_arn: String,
}
fn check_arn(check_arn: CheckArnParam) -> Result<bool, Error> {
    let resource_arn: String = env::var("RESOURCE_ARN").unwrap().as_str().to_owned();
    let secret_arn: String = env::var("SECRET_ARN").unwrap().as_str().to_owned();
    if check_arn.resource_arn != resource_arn {
        Err(Error {
            msg: format!("HttpEndPoint is not enabled for {}", resource_arn),
            status: 400,
        })
    } else if check_arn.secret_arn != secret_arn {
        Err(Error {
            msg: "Invalid secret_arn".to_owned(),
            status: 400,
        })
    } else {
        Ok(true)
    }
}

#[post("/BeginTransaction")]
async fn begin_transaction_statement(begin_transaction_request_wj: web::Json<BeginTransactionRequest>, app_data: web::Data<AppData>) -> Result<HttpResponse, Error> {
    let begin_transaction_request = begin_transaction_request_wj.into_inner();
    check_arn(CheckArnParam {
        resource_arn: begin_transaction_request.resource_arn,
        secret_arn: begin_transaction_request.secret_arn,
    })?;
    let transaction_id = create_transaction_id();
    let begin_transaction_response = BeginTransactionResponse {
        transaction_id: transaction_id.clone()
    };
    let mut conn = get_mysql_conn();
    // conn.query("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")?;
    conn.query("START TRANSACTION")?;
    select_database_and_schema(&mut conn, begin_transaction_request.database, begin_transaction_request.schema).expect("select db failed");
    let mut connections = app_data.connections.lock().unwrap();
    connections.insert(transaction_id.clone(), conn);

    Ok(HttpResponse::Ok()
        .content_type("application/json")
        .json(begin_transaction_response))
}

// no escape yet, no select schema yet
fn select_database_and_schema(conn: &mut mysql::Conn, database: Option<String>, _schema: Option<String>) -> Result<bool, Error> {
    if let Some(db) = database {
        conn.query(format!("USE {}", db))?;
    }
    Ok(true)
}

#[post("/CommitTransaction")]
async fn commit_transaction_statement(commit_transaction_request_wj: web::Json<CommitTransactionRequest>, app_data: web::Data<AppData>) -> Result<HttpResponse, Error> {
    let commit_transaction_request = commit_transaction_request_wj.into_inner();
    check_arn(CheckArnParam {
        resource_arn: commit_transaction_request.resource_arn,
        secret_arn: commit_transaction_request.secret_arn,
    })?;
    let mut connections = app_data.connections.lock().unwrap();
    if !connections.contains_key(&commit_transaction_request.transaction_id) {
        return Err(Error {
            msg: "Invalid transaction ID".to_string(),
            status: 400,
        })
    }
    {
        let conn = connections.get_mut(&commit_transaction_request.transaction_id).unwrap();
        conn.query("COMMIT")?;
    }
    if let Some(con) = connections.remove(&commit_transaction_request.transaction_id) {
        drop(con);
    }
    Ok(HttpResponse::Ok()
        .json(CommitTransactionResponse {
            transaction_status: TransactionStatus::TransactionCommitted,
        }))
}

#[post("/RollbackTransaction")]
async fn rollback_transaction_statement(rollback_transaction_request_wj: web::Json<RollbackTransactionRequest>, app_data: web::Data<AppData>) ->  Result< HttpResponse, Error> {
    let rollback_transaction_request = rollback_transaction_request_wj.into_inner();
    check_arn(CheckArnParam {
        resource_arn: rollback_transaction_request.resource_arn,
        secret_arn: rollback_transaction_request.secret_arn,
    })?;
    let mut connections = app_data.connections.lock().unwrap();
    if !connections.contains_key(&rollback_transaction_request.transaction_id) {
        return Err(Error {
            msg: "Invalid transaction ID".to_string(),
            status: 400,
        })
    }
    {
        let conn = connections.get_mut(&rollback_transaction_request.transaction_id).unwrap();
        conn.query("Rollback")?;
    }
    connections.remove(&rollback_transaction_request.transaction_id);
    if let Some(con) = connections.remove(&rollback_transaction_request.transaction_id) {
        drop(con);
    }
    Ok(HttpResponse::Ok()
        .json(RollbackTransactionResponse {
            transaction_status: TransactionStatus::RollbackComplete,
        }))
}
fn put_param_to_hashmap(hashmap: &mut HashMap::<String, MysqlValue, BuildHasherDefault<XxHash>>, paramnamemap: &HashMap<String, String>, _sqlstr: &String, parameters: Vec<SqlParameter>, _originalsql: &String)-> Result<(), Error> {
    for parameter in parameters {
        match paramnamemap.get(&parameter.name) {
            Some(snake_name) => {
                match parameter.value {
                    Field::BlobValue(value) => {
                        hashmap.insert(snake_name.to_string(), MysqlValue::Bytes(value.into_bytes()));
                    },
                    Field::BooleanValue(value) => {
                        let boolint: u64 = if value {
                            1
                        } else {
                            0
                        };
                        hashmap.insert(snake_name.to_string(), MysqlValue::UInt(boolint));
                    },
                    Field::DoubleValue(value) => {
                        hashmap.insert(snake_name.to_string(), MysqlValue::Float(value));
                    },
                    Field::IsNull(_) => {
                        hashmap.insert(snake_name.to_string(), MysqlValue::NULL);
                    },
                    Field::LongValue(value) => {
                        hashmap.insert(snake_name.to_string(), MysqlValue::Int(value));
                    },
                    Field::StringValue(value) => {
                        hashmap.insert(snake_name.to_string(), MysqlValue::Bytes(value.into_bytes()));
                    },
                }
            },
            None => {
                // println!("{:?} {:?}", parameter.name, paramnamemap);
                // return Err(Error{
                //     msg: format!("invalid sql: {}, debug: {}", originalsql, sqlstr),
                //     status: 400
                // })
            }
        }
    }
    Ok(())
}
fn format_sql_to_snake(sqlstr: String) -> (String, HashMap<String, String>){
    // for rust-mysql v17 issue with camel case param name
    let sqlvec: Vec<char> = sqlstr.chars().collect();
    let mut targetsqlvec: Vec<char> = Vec::new();
    let mut paramnamemap: HashMap<String, String> = HashMap::new();
    let mut parampostfix = 0;
    let mut value_quote = ' ';
    let mut is_escape = false;
    let mut is_param = false;
    let mut paramidx = (0, 0);
    for idx in 0..sqlvec.len() {
        let ch = sqlvec[idx];
        let mut proceed_with_paramname = false;
        if is_escape {
            is_escape = false;
        } else if ch == '\\' {
            is_escape = true;
        } else if value_quote == ' ' && (ch == '\'' || ch == '\"') {
            value_quote = ch;
        } else if value_quote != ' ' && ch == value_quote {
            if ch == '\'' && idx != sqlvec.len() - 1 && sqlvec[idx + 1] == '\'' {
                is_escape = true;
            } else {
                value_quote = ' ';
            }
        } else {
            proceed_with_paramname = true;
        }
        // println!("ch {}, value quote {} proceed_with_paramname {}", ch, value_quote, proceed_with_paramname);
        if value_quote != ' ' || proceed_with_paramname == false {
            targetsqlvec.push(ch);
            continue;
        }
        if is_param && !(char::is_ascii_alphanumeric(&ch) || ch == '_') {
            paramidx.1 = idx - 1;
            is_param = false;
            let param: String = (&sqlvec[(paramidx.0)..(paramidx.1 + 1)]).iter().collect();
            let st: String = match paramnamemap.get(&param) {
                Some(o) => o.to_string(),
                None => {
                    let s1 = format!("q{}", parampostfix);
                    paramnamemap.insert(param.clone(), s1.clone());
                    parampostfix += 1;
                    s1
                }
            };
            targetsqlvec.push(':');
            for pch in st.chars() {
                targetsqlvec.push(pch);
            }
        } else if ch == ':' && idx != sqlvec.len() - 1 && char::is_ascii_alphanumeric(&sqlvec[idx+1]) {
            is_param = true;
            paramidx.0 = idx + 1;
        }
        if !is_param {
            targetsqlvec.push(ch);
        }
    }
    if is_param {
        paramidx.1 = sqlstr.len() - 1;
        let param: String = (&sqlvec[(paramidx.0)..(paramidx.1 + 1)]).iter().collect();
        let st: String = match paramnamemap.get(&param) {
            Some(o) => o.to_string(),
            None => {
                let s1 = format!("q{}", parampostfix);
                paramnamemap.insert(param.clone(), s1.clone());
                s1
            }
        };
        targetsqlvec.push(':');
        for pch in st.chars() {
            targetsqlvec.push(pch);
        }
    }
    (targetsqlvec.into_iter().collect(), paramnamemap)
}
#[post("/Execute")]
async fn execute_statement(execute_transaction_request_wj: web::Json<ExecuteStatementRequest>, app_data: web::Data<AppData>) ->  Result<HttpResponse, Error> {
    let execute_transaction_request = execute_transaction_request_wj.into_inner();
    check_arn(CheckArnParam {
        resource_arn: execute_transaction_request.resource_arn,
        secret_arn: execute_transaction_request.secret_arn,
    })?;

    let (sqlstr, paramnamemap) = format_sql_to_snake(execute_transaction_request.sql.clone());

    let params = match execute_transaction_request.parameters {
        Some(parameters) => {
            let mut hashmap = HashMap::<String, MysqlValue, BuildHasherDefault<XxHash>>::default();
            if parameters.len() > 0 {
                put_param_to_hashmap(&mut hashmap, &paramnamemap, &sqlstr, parameters, &execute_transaction_request.sql)?;
                mysql::Params::Named(hashmap)
            } else {
                mysql::Params::Empty
            }
        },
        None => mysql::Params::Empty,
    };
    let include_result_metadata = match execute_transaction_request.include_result_metadata {
        Some(b) => b,
        None => false,
    };
    // println!("{} {:?}", sqlstr, params);
    let exec_result = if execute_transaction_request.transaction_id.is_none() {
        let mut conn = get_mysql_conn();
        select_database_and_schema(&mut conn, execute_transaction_request.database, execute_transaction_request.schema).expect("select db failed");
        let mut result = if params == mysql::Params::Empty {
            conn.query(sqlstr)?
        } else {
            conn.prep_exec(sqlstr, params)?
        };
        format_prep_exec_result(&mut result, include_result_metadata)
    } else {
        let transaction_id = execute_transaction_request.transaction_id.unwrap();
        let mut connections = app_data.connections.lock().unwrap();
        if !connections.contains_key(&transaction_id) {
            return Err(Error {
                msg: "Invalid transaction ID".to_string(),
                status: 400,
            })
        }
        let mut conn = connections.get_mut(&transaction_id).unwrap();
        select_database_and_schema(&mut conn, execute_transaction_request.database, execute_transaction_request.schema).expect("select db failed");
        let mut result = if params == mysql::Params::Empty {
            conn.query(sqlstr)?
        } else {
            conn.prep_exec(sqlstr, params)?
        };
        format_prep_exec_result(&mut result, include_result_metadata)
    };
    match exec_result {
        Ok(some) => Ok(HttpResponse::Ok().json(some)),
        Err(some_error) => Err(some_error),
    }
}
fn format_prep_exec_result(query_result: &mut mysql::QueryResult, include_result_metadata: bool) -> Result<ExecuteStatementResponse, Error> {
    let mut records: Vec<Vec<Field>> = Vec::new();
    let mut column_metadata: Vec<ColumnMetadata> = Vec::new();
    let need_column_metadata = if include_result_metadata {
        true
    } else {
        false
    };
    let mut column_types: Vec<MappedMysqlColumnType> = Vec::new();
    let generated_fields: Option<Vec<Field>> = if query_result.last_insert_id() == 0 {
        None
    } else {
        Some(vec![Field::LongValue(query_result.last_insert_id() as i64)])
    };
    for x in query_result.columns_ref() {
        let column_type = x.column_type();
        let column_flags = x.flags();
        let mapped_mysql_column_type = map_mysql_column_type(column_type, column_flags);
        column_types.push(mapped_mysql_column_type.clone());
        if need_column_metadata {
            column_metadata.push(ColumnMetadata {
                array_base_column_type: None,
                is_auto_increment: None,
                is_case_sensitive: None,
                is_currency: None,
                is_signed: None,
                label: Some(x.name_str().to_string()),
                name: Some(x.org_name_str().to_string()),
                nullable: None,
                precision: None,
                scale: None,
                schema_name: Some(x.schema_str().to_string()),
                table_name: Some(x.table_str().to_string()),
                type_: None,
                type_name: Some(mapped_mysql_column_type.type_name.clone()),
            })
        };
    }
    while query_result.more_results_exists() {
        for x in query_result.by_ref() {
            let mut record: Vec<Field> = Vec::new();
            let row = x.unwrap();
            for i in 0..row.len() {
                let mapped_mysql_column_type = &column_types[i];
                let field_option: Result<Field, String> = match row.as_ref(i).unwrap() {
                    MysqlValue::NULL => {
                        Ok(Field::IsNull(true))
                    },
                    MysqlValue::Int(value) => {
                        let val = value.clone();
                        Ok(Field::LongValue(val))
                    },
                    MysqlValue::UInt(value) => {
                        let val = value.clone();
                        Ok(Field::LongValue(val as i64))
                    },
                    MysqlValue::Float(value) => {
                        Ok(Field::DoubleValue(value.clone() as f64))
                    },
                    MysqlValue::Date(dy, dm, dd, h, m, s, _ms) => {
                        Ok(Field::StringValue(format!("{}-{:02}-{:02} {:02}:{:02}:{:02}", dy, dm, dd, h, m, s)))
                    },
                    MysqlValue::Time(_is_negative, _d, h, m, s, _ms) => {
                        Ok(Field::StringValue(format!("{:02}:{:02}:{:02}", h, m, s)))
                    },
                    MysqlValue::Bytes(byte) => {
                        match mapped_mysql_column_type.column_field {
                            ColumnField::StringValue => {
                                let stringvalue = String::from_utf8_lossy(byte).to_string();
                                Ok(Field::StringValue(stringvalue))
                            },
                            ColumnField::BlobValue => {
                                Err(format!("not implemented conversion of Value from BlobValue"))
                            },
                            ColumnField::BooleanValue => {
                                let stringvalue = String::from_utf8_lossy(byte).to_string();
                                if stringvalue == "0".to_owned() {
                                    Ok(Field::BooleanValue(false))
                                } else if stringvalue == "1".to_owned() {
                                    Ok(Field::BooleanValue(true))
                                } else {
                                    Err(format!("unexpected convertion to boolean error from {:?}", stringvalue))
                                }
                            },
                            ColumnField::IsNull => {
                                Ok(Field::IsNull(true))
                            },
                            ColumnField::LongValue => {
                                let stringvalue = String::from_utf8_lossy(byte).to_string();
                                Ok(Field::LongValue(stringvalue.parse::<i64>().unwrap()))
                            },
                            ColumnField::DoubleValue => {
                                let stringvalue = String::from_utf8_lossy(byte).to_string();
                                Ok(Field::DoubleValue(stringvalue.parse::<f64>().unwrap()))
                            },
                        }
                    },
                };
                match field_option {
                    Ok(field) => record.push(field),
                    Err(msg) => panic!(msg),
                }
            }
            records.push(record);
        }
    }
    Ok(ExecuteStatementResponse {
        number_of_records_updated: query_result.affected_rows(),
        generated_fields: generated_fields,
        records: Some(records),
        column_metadata: Some(column_metadata),
    })
}

fn format_batch_exec_result(query_result: &mut mysql::QueryResult) -> Result<Vec<Field>, Error> {
    let generated_fields: Vec<Field> = vec![Field::LongValue(query_result.last_insert_id() as i64)];
    Ok(generated_fields)
}
#[post("/BatchExecute")]
async fn batch_execute_statement(batch_execute_transaction_request_wj: web::Json<BatchExecuteStatementRequest>, app_data: web::Data<AppData>) ->  Result<HttpResponse, Error> {
    let batch_execute_transaction_request = batch_execute_transaction_request_wj.into_inner();
    check_arn(CheckArnParam {
        resource_arn: batch_execute_transaction_request.resource_arn,
        secret_arn: batch_execute_transaction_request.secret_arn,
    })?;
    // let include_result_metadata = match batch_execute_transaction_request.include_result_metadata {
    //     Some(b) => b,
    //     None => false,
    // };

    let (sqlstr, paramnamemap) = format_sql_to_snake(batch_execute_transaction_request.sql.clone());

    let param_sets = match batch_execute_transaction_request.parameter_sets {
        Some(parameter_sets) => {
            let mut vec_params: Vec<MysqlParams> = Vec::with_capacity(parameter_sets.len());
            for parameters in parameter_sets {
                let mut hashmap = HashMap::<String, MysqlValue, BuildHasherDefault<XxHash>>::default();
                put_param_to_hashmap(&mut hashmap, &paramnamemap, &sqlstr, parameters, &batch_execute_transaction_request.sql)?;

                vec_params.push(MysqlParams::Named(hashmap));
            }
            vec_params
        },
        None => {
            let vec_params: Vec<MysqlParams> = Vec::with_capacity(0);
            vec_params
        },
    };

    let mut update_results: Vec<UpdateResult> = Vec::with_capacity(param_sets.len());
    let _exec_result: Result<bool, Error> = if batch_execute_transaction_request.transaction_id.is_none() {
        let mut conn = get_mysql_conn();
        select_database_and_schema(&mut conn, batch_execute_transaction_request.database, batch_execute_transaction_request.schema).expect("select db failed");
        for params in param_sets {
            let mut result = conn.prep_exec(sqlstr.clone(), params)?;
            let generated_fields = format_batch_exec_result(&mut result)?;
            update_results.push(UpdateResult {
                generated_fields: generated_fields,
            });
        }
        Ok(true)
    } else {
        let transaction_id = batch_execute_transaction_request.transaction_id.unwrap();
        let mut connections = app_data.connections.lock().unwrap();
        if !connections.contains_key(&transaction_id) {
            return Err(Error {
                msg: "Invalid transaction ID".to_string(),
                status: 400,
            });
        }
        let mut conn = connections.get_mut(&transaction_id).unwrap();
        select_database_and_schema(&mut conn, batch_execute_transaction_request.database, batch_execute_transaction_request.schema).expect("select db failed");
        for params in param_sets {
            let mut result = conn.prep_exec(sqlstr.clone(), params)?;
            let generated_fields = format_batch_exec_result(&mut result)?;
            update_results.push(UpdateResult {
                generated_fields: generated_fields,
            });
        }
        Ok(true)
    };
    Ok(HttpResponse::Ok().json(BatchExecuteStatementResponse {
        update_results: update_results,
    }))
}

#[get("/")]
async fn root_index(_req: HttpRequest) -> Result<HttpResponse, Error> {
    let data = Field::StringValue(String::from("ok"));
    Ok(
    HttpResponse::Ok()
    .json(data))
}
#[derive(Clone)]
struct AppData {
    // should not lock entire hashmap if want really fast performance
    connections: Arc<Mutex<HashMap<String, mysql::Conn>>>,
}

fn create_transaction_id() -> String {
    const TRANSACTION_ID_CHARACTERS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ\
                            abcdefghijklmnopqrstuvwxyz\
                            /=+";
    const TRANSACTION_ID_LENGTH: usize = 184;
    let mut rng = rand::thread_rng();
    (0..TRANSACTION_ID_LENGTH)
        .map(|_| {
            let idx = rng.gen_range(0, TRANSACTION_ID_CHARACTERS.len());
            TRANSACTION_ID_CHARACTERS[idx] as char
        })
        .collect()
}
fn get_mysql_conn() -> mysql::Conn {
    let mysql_host: String = env::var("MYSQL_HOST").unwrap().as_str().to_owned();
    let mysql_port: String = env::var("MYSQL_PORT").unwrap().as_str().to_owned();
    let mysql_user: String = env::var("MYSQL_USER").unwrap().as_str().to_owned();
    let mysql_password: String = env::var("MYSQL_PASSWORD").unwrap().as_str().to_owned();
    mysql::Conn::new(format!("mysql://{}:{}@{}:{}/mysql", mysql_user, mysql_password, mysql_host, mysql_port)).unwrap()
}

#[actix_rt::main]
async fn main() -> io::Result<()> {
    dotenv().ok();
    env_logger::init();

    let app_data = AppData {
        connections: Arc::new(Mutex::new(HashMap::new())),
    };
    let json_limit = env::var("JSONLIMIT").unwrap().as_str().parse::<usize>().unwrap();
    println!("Starting http server: {}:{}", env::var("HOST").unwrap().as_str().to_owned(), env::var("PORT").unwrap().as_str().to_owned());
    HttpServer::new(move || {
        App::new()
            .app_data(web::JsonConfig::default().limit(json_limit)) // <- limit size of the payload (global configuration)
            // enable logger - always register actix-web Logger middleware last
            .wrap(middleware::Logger::default())
            .data(app_data.clone())
            // register simple route, handle all methods
            .service(root_index)
            .service(begin_transaction_statement)
            .service(commit_transaction_statement)
            .service(rollback_transaction_statement)
            .service(execute_statement)
            .service(batch_execute_statement)
            .default_service(
                // 404 for GET request
                web::resource("")
                    .route(web::get().to(p404))
                    // all requests that are not `GET`
                    .route(
                        web::route()
                            .guard(guard::Not(guard::Get()))
                            .to(HttpResponse::MethodNotAllowed),
                    ),
            )
    })
    .bind(format!("{}:{}", env::var("HOST").unwrap().as_str().to_owned(), env::var("PORT").unwrap().as_str().to_owned()))?
    .run()
    .await
}
