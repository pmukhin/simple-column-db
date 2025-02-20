use std::collections::BTreeMap;
use crate::data::Data;

pub struct SSTable {
    data: BTreeMap<String, Vec<Data>>,
}

impl Default for SSTable {
    fn default() -> Self {
        SSTable {
            data: BTreeMap::new(),
        }
    }
}

impl SSTable {
    fn new() -> Self {
        SSTable {
            data: BTreeMap::new(),
        }
    }

    pub fn insert(&mut self, key: String, value: &[Data]) {
        self.data.insert(key, value.to_owned());
    }

    pub fn read_all(&self, limit: usize) -> Vec<Data> {
        self.data.values().take(limit).cloned().flatten().collect()
    }
}

#[cfg(test)]
mod tests {
    use crate::sstable::SSTable;

    #[test]
    fn test_sstable() {
        let mut sstable = SSTable::new();
        for i in 1..100 {
            sstable.data.insert(format!("key_{}", i), vec![]);
        }
        let mut keys = sstable.data.keys().collect::<Vec<_>>();
        assert!(keys.is_sorted());
    }
}
