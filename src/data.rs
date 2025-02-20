#[derive(Clone, Debug)]
pub enum Data {
    String(String),
    Integer(i64),
}

#[derive(Debug, Clone)]
pub enum Schema {
    String(usize),
    Integer,
    All,
}
