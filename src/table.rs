use crate::data::{Data, Schema};
use crate::sstable::SSTable;

#[derive(Default)]
pub struct Table {
    pub name: String,
    pub schema: Vec<Schema>,

    // private
    sstable: SSTable,
}

unsafe impl Send for Table {}

enum Error {
    IncompatibleSchema,
}

impl Table {
    pub fn new(name: String, schema: Vec<Schema>) -> Table {
        Table {
            name,
            schema,
            ..Default::default()
        }
    }

    pub fn read_all(&self, limit: usize) -> Vec<Data> {
        self.sstable.read_all(limit)
    }

    pub fn insert(&mut self, key: String, value: &[Data]) -> Result<(), Error> {
        if value.len() != self.schema.len() {
            return Err(Error::IncompatibleSchema);
        }

        value
            .iter()
            .enumerate()
            .zip(&self.schema)
            .map(|((p, d), s)| {
                match (d, s) {
                    (Data::String(d), Schema::String(s_size)) => {
                        if d.len() > *s_size { Err(Error::IncompatibleSchema) } else { Ok(()) }
                    }
                    (Data::Integer(_), Schema::Integer) => Ok(()),
                    (d_type, s_type) => Err(Error::IncompatibleSchema)
                }
            })
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| Error::IncompatibleSchema)?;

        Ok(self.sstable.insert(key, value))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_schema() {
        let mut table = Table {
            name: "fancy_table".to_string(),
            schema: vec![Schema::String(12), Schema::String(5), Schema::Integer],
            sstable: Default::default(),
        };
        let result =
            table.insert("sdfsdf".to_string(),
                         &vec![
                             Data::String("84393sdfd78".to_string()),
                             Data::String("12313".to_string()),
                             Data::Integer(123),
                         ],
            );
        assert!(result.is_ok());
    }
}
