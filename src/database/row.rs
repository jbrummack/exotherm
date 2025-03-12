use std::collections::HashMap;

use super::values_indices::DbValue;

#[allow(unused)]
pub struct StoredRow(HashMap<u16, Column>);
#[allow(unused)]
pub struct Column(u16, DbValue);
#[allow(unused)]
pub struct Row(HashMap<u16, Column>);
#[allow(unused)]
impl Row {
    pub fn from_columns(cols: Vec<Column>) {}
    pub fn get(&self, id: u16) {}
}
