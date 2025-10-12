use introspect_types::{EnumDef, TypeDef};

pub enum SqlLiteTypes {
    Null,
    Integer,
    Text,
    Blob,
}

pub struct SqlLiteColumn {
    pub path: Vec<String>,
    pub sql_type: SqlLiteTypes,
}

impl SqlLiteColumn {
    fn new(path: Vec<String>, sql_type: SqlLiteTypes) -> Self {
        SqlLiteColumn {
            path: path.iter().map(|s| s.to_string()).collect(),
            sql_type,
        }
    }
    fn new_vec(path: Vec<String>, sql_type: SqlLiteTypes) -> Vec<Self> {
        vec![SqlLiteColumn::new(path, sql_type)]
    }
}

trait SqlLiteFlatten {
    type Output;
    fn sql_flatten(&self, prefixes: Vec<String>) -> Vec<Self::Output>;
}

fn flatten_tuple(types: &Vec<TypeDef>, prefixes: Vec<String>) -> Vec<SqlLiteColumn> {
    let mut columns = vec![];
    for (i, type_def) in types.iter().enumerate() {
        let mut new_prefixes = prefixes.clone();
        new_prefixes.push(format!("{}", i));
        columns.extend(type_def.sql_flatten(new_prefixes));
    }
    columns
}

impl SqlLiteFlatten for EnumDef {
    type Output = SqlLiteColumn;

    fn sql_flatten(&self, prefixes: Vec<String>) -> Vec<Self::Output> {
        let mut columns = vec![];
        let mut new_prefixes = prefixes.clone();
        new_prefixes.push("variant".to_string());
        columns.push(SqlLiteColumn::new(new_prefixes, SqlLiteTypes::Integer));
        for variant in &self.variants {
            let mut variant_prefixes = prefixes.clone();
            variant_prefixes.push(variant.name.clone());
            columns.extend(variant.type_def.sql_flatten(variant_prefixes));
        }
        columns
    }
}

impl SqlLiteFlatten for TypeDef {
    type Output = SqlLiteColumn;

    fn sql_flatten(&self, prefixes: Vec<String>) -> Vec<Self::Output> {
        match self {
            TypeDef::None => vec![],
            TypeDef::Bool
            | TypeDef::U8
            | TypeDef::U16
            | TypeDef::U32
            | TypeDef::I8
            | TypeDef::I16
            | TypeDef::I32
            | TypeDef::I64 => SqlLiteColumn::new_vec(prefixes, SqlLiteTypes::Integer),
            TypeDef::Felt252
            | TypeDef::ShortString
            | TypeDef::ClassHash
            | TypeDef::ContractAddress
            | TypeDef::EthAddress
            | TypeDef::ByteArray
            | TypeDef::U64
            | TypeDef::U128
            | TypeDef::U256
            | TypeDef::I128 => SqlLiteColumn::new_vec(prefixes, SqlLiteTypes::Text),
            TypeDef::Tuple(types) => flatten_tuple(types, prefixes),
            _ => unimplemented!(), // Default to Blob for unsupported types
        }
    }
}
