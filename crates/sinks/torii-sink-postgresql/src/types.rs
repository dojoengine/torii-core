enum PostgreSQLType {
    Boolean,
    SmallInt, // u16
    Int,      // u32
    BigInt,   // u64
    Numeric(u32, u32),
    Text,
    Char(u32),
    Varchar(u32),
    Json,
    JsonB,
    Enum(Vec<String>),
    Array(Box<PostgreSQLType>),
    FixedArray(Box<PostgreSQLType>, u32),
}

struct ColumnDef {
    name: String,
    data_type: PostgreSQLType,
}

trait PostgreSQLFlatten {
    fn flatten(&self, base: &str) -> Vec<PostgreSQLType>;
}

impl PostgreSQLFlatten for Vec<ColumnDef> {
    fn flatten(&self) -> Vec<PostgreSQLType> {
        self.iter().map(|col| col.data_type.clone()).collect()
    }
}
