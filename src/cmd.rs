use crate::data::{Data, Schema};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use sqlparser::parser::ParserError;
use sqlparser::ast::{Expr, SetExpr, Statement};
use sqlparser::ast::Value::{Number, SingleQuotedString};

#[derive(Debug)]
pub enum Command {
    CreateTable { name: String, columns: Vec<Schema> },
    Select { name: String, columns: Vec<String> },
    Insert { name: String, columns: Vec<String>, values: Vec<Data> },
    Update { name: String, columns: Vec<String> },
}

#[derive(Debug)]
pub enum Error {
    Parse(ParserError),
    Unsupported(String),
}

impl From<ParserError> for Error {
    fn from(err: ParserError) -> Error {
        Error::Parse(err)
    }
}

impl Command {
    // create table if not exist awesome_table (
    //  id varchar(255) primary key not null,
    //  counter integer not null
    // )
    // select * from <table> where id = "id"
    pub fn parse(sql: &str) -> Result<Self, Error> {
        let mut cmd = Parser::parse_sql(&GenericDialect, sql)?;
        match &cmd[0] {
            Statement::Query(q) => {
                let select =
                    q.body.as_select().ok_or(Error::Unsupported("query isn't a Select".to_string()))?;
                let from = &select.from;
                if from.len() > 1 {
                    return Err(Error::Unsupported("select query should contain exactly 1 table".to_string()));
                }
                let first = from.first()
                    .ok_or(Error::Unsupported("no first table?".to_string()))?;

                let columns: Vec<String> =
                    select.projection
                        .iter().map(|s_item| s_item.clone().to_string())
                        .collect::<Vec<_>>();

                Ok(Command::Select { name: first.relation.to_string(), columns })
            }
            Statement::CreateTable(_) => {
                Ok(Command::CreateTable { name: "".to_string(), columns: vec![] })
            }
            Statement::Insert(q) => {
                let table_name = q.table.to_string();
                let columns = q.columns.iter().map(|i| i.to_string()).collect();
                let as_sources =
                    q.source.as_deref().ok_or(Error::Unsupported("query isn't a Source".to_string()))?;

                match *as_sources.body {
                    SetExpr::Values(ref val) => {
                        let values_or_errors =
                            val.rows[0].iter()
                                .map(|v| {
                                    match v {
                                        Expr::Value(SingleQuotedString(v)) => {
                                            Ok(Data::String(v.to_string()))
                                        }
                                        Expr::Value(Number(v, _)) => {
                                            Ok(Data::Integer(v.parse::<i64>().unwrap()))
                                        }
                                        _ => Err(Error::Unsupported("only strings and numbers are supported".to_string())),
                                    }
                                })
                                .collect::<Result<Vec<_>, _>>();
                        let values: Vec<Data> = values_or_errors?;
                        Ok(Command::Insert { name: table_name, columns, values })
                    }
                    _ => Err(Error::Unsupported("only VALUES are supported".to_string()))
                }
            }
            Statement::Update { .. } => {
                Ok(Command::Update { name: "".to_string(), columns: vec![] })
            }
            _ => Err(Error::Unsupported("unsupported query".to_string())),
        }
    }
}
