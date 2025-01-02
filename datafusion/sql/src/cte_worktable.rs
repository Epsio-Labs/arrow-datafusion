use std::any::Any;
use std::sync::Arc;
use arrow_schema::SchemaRef;
use datafusion_expr::{Expr, LogicalPlan, TableProviderFilterPushDown, TableSource, TableType};

/// The temporary working table where the previous iteration of a recursive query is stored
/// Naming is based on PostgreSQL's implementation.
/// See here for more details: www.postgresql.org/docs/11/queries-with.html#id-1.5.6.12.5.4
#[derive(Debug)]
pub struct CteWorkTable {
    /// The name of the CTE work table
    name: String,
    /// This schema must be shared across both the static and recursive terms of a recursive query
    table_schema: SchemaRef,
}

impl CteWorkTable {
    /// construct a new CteWorkTable with the given name and schema
    /// This schema must match the schema of the recursive term of the query
    /// Since the scan method will contain an physical plan that assumes this schema
    pub fn new(name: &str, table_schema: SchemaRef) -> Self {
        Self {
            name: name.to_owned(),
            table_schema,
        }
    }

    /// The user-provided name of the CTE
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The schema of the recursive term of the query
    pub fn schema(&self) -> SchemaRef {
        Arc::clone(&self.table_schema)
    }
}

impl TableSource for CteWorkTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Get a reference to the schema for this table
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.table_schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Temporary
    }

    fn supports_filter_pushdown(&self, _filter: &Expr) -> datafusion_common::Result<TableProviderFilterPushDown> {
        Ok(
            TableProviderFilterPushDown::Unsupported
        )
    }

    fn supports_filters_pushdown(&self, filters: &[&Expr]) -> datafusion_common::Result<Vec<TableProviderFilterPushDown>> {
        Ok(
            filters.iter().map(|_| TableProviderFilterPushDown::Unsupported).collect()
        )
    }

    fn get_logical_plan(&self) -> Option<&LogicalPlan> {
        None
    }
}
