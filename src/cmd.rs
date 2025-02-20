use crate::data::{Data, Schema};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use sqlparser::parser::ParserError;
use sqlparser::ast::Statement;

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

                Ok(Command::Insert { name: table_name, columns, values: vec![] })
            }
            Statement::Update { .. } => {
                Ok(Command::Update { name: "".to_string(), columns: vec![] })
            }
            // Statement::Analyze { .. } => {}
            // Statement::Truncate { .. } => {}
            // Statement::Msck { .. } => {}
            // Statement::Install { .. } => {}
            // Statement::Load { .. } => {}
            // Statement::Directory { .. } => {}
            // Statement::Call(_) => {}
            // Statement::Copy { .. } => {}
            // Statement::CopyIntoSnowflake { .. } => {}
            // Statement::Close { .. } => {}
            // Statement::Delete(_) => {}
            // Statement::CreateView { .. } => {}
            // Statement::CreateVirtualTable { .. } => {}
            // Statement::CreateIndex(_) => {}
            // Statement::CreateRole { .. } => {}
            // Statement::CreateSecret { .. } => {}
            // Statement::CreatePolicy { .. } => {}
            // Statement::AlterTable { .. } => {}
            // Statement::AlterIndex { .. } => {}
            // Statement::AlterView { .. } => {}
            // Statement::AlterRole { .. } => {}
            // Statement::AlterPolicy { .. } => {}
            // Statement::AttachDatabase { .. } => {}
            // Statement::AttachDuckDBDatabase { .. } => {}
            // Statement::DetachDuckDBDatabase { .. } => {}
            // Statement::Drop { .. } => {}
            // Statement::DropFunction { .. } => {}
            // Statement::DropProcedure { .. } => {}
            // Statement::DropSecret { .. } => {}
            // Statement::DropPolicy { .. } => {}
            // Statement::Declare { .. } => {}
            // Statement::CreateExtension { .. } => {}
            // Statement::DropExtension { .. } => {}
            // Statement::Fetch { .. } => {}
            // Statement::Flush { .. } => {}
            // Statement::Discard { .. } => {}
            // Statement::SetRole { .. } => {}
            // Statement::SetVariable { .. } => {}
            // Statement::SetTimeZone { .. } => {}
            // Statement::SetNames { .. } => {}
            // Statement::SetNamesDefault { .. } => {}
            // Statement::ShowFunctions { .. } => {}
            // Statement::ShowVariable { .. } => {}
            // Statement::ShowStatus { .. } => {}
            // Statement::ShowVariables { .. } => {}
            // Statement::ShowCreate { .. } => {}
            // Statement::ShowColumns { .. } => {}
            // Statement::ShowDatabases { .. } => {}
            // Statement::ShowSchemas { .. } => {}
            // Statement::ShowTables { .. } => {}
            // Statement::ShowViews { .. } => {}
            // Statement::ShowCollation { .. } => {}
            // Statement::Use(_) => {}
            // Statement::StartTransaction { .. } => {}
            // Statement::SetTransaction { .. } => {}
            // Statement::Comment { .. } => {}
            // Statement::Commit { .. } => {}
            // Statement::Rollback { .. } => {}
            // Statement::CreateSchema { .. } => {}
            // Statement::CreateDatabase { .. } => {}
            // Statement::CreateFunction(_) => {}
            // Statement::CreateTrigger { .. } => {}
            // Statement::DropTrigger { .. } => {}
            // Statement::CreateProcedure { .. } => {}
            // Statement::CreateMacro { .. } => {}
            // Statement::CreateStage { .. } => {}
            // Statement::Assert { .. } => {}
            // Statement::Grant { .. } => {}
            // Statement::Revoke { .. } => {}
            // Statement::Deallocate { .. } => {}
            // Statement::Execute { .. } => {}
            // Statement::Prepare { .. } => {}
            // Statement::Kill { .. } => {}
            // Statement::ExplainTable { .. } => {}
            // Statement::Explain { .. } => {}
            // Statement::Savepoint { .. } => {}
            // Statement::ReleaseSavepoint { .. } => {}
            // Statement::Merge { .. } => {}
            // Statement::Cache { .. } => {}
            // Statement::UNCache { .. } => {}
            // Statement::CreateSequence { .. } => {}
            // Statement::CreateType { .. } => {}
            // Statement::Pragma { .. } => {}
            // Statement::LockTables { .. } => {}
            // Statement::UnlockTables => {}
            // Statement::Unload { .. } => {}
            // Statement::OptimizeTable { .. } => {}
            // Statement::LISTEN { .. } => {}
            // Statement::UNLISTEN { .. } => {}
            // Statement::NOTIFY { .. } => {}
            // Statement::LoadData { .. } => {}
            // Statement::RenameTable(_) => {}
            // Statement::List(_) => {}
            // Statement::Remove(_) => {}
            // Statement::SetSessionParam(_) => {}
            // Statement::RaisError { .. } => {}
            _ => Err(Error::Unsupported("unsupported query".to_string())),
        }
    }
}
