// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::sync::Arc;
use crate::planner::{ContextProvider, PlannerContext, SqlToRel};
use arrow::datatypes::Schema;
use datafusion_common::{
    not_impl_err, plan_err, sql_err, Constraints, DataFusionError, Result, ScalarValue,
};
use datafusion_expr::expr_rewriter::normalize_col;
use datafusion_expr::{CreateMemoryTable, DdlStatement, Expr, LogicalPlan, LogicalPlanBuilder, TableSource};
use sqlparser::ast::{Cte, Expr as SQLExpr, Offset as SQLOffset, OrderByExpr, Query, SetExpr, SetOperator, SetQuantifier, Value};

use crate::utils::{extract_aliases, resolve_aliases_to_exprs};
use sqlparser::parser::ParserError::ParserError;
use crate::cte_worktable::CteWorkTable;

impl<'a, S: ContextProvider> SqlToRel<'a, S> {
    /// Generate a logical plan from an SQL query
    pub(crate) fn query_to_plan(
        &self,
        query: Query,
        planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        self.query_to_plan_with_schema(query, planner_context)
    }

    /// Generate a logic plan from an SQL query.
    /// It's implementation of `subquery_to_plan` and `query_to_plan`.
    /// It shouldn't be invoked directly.
    fn query_to_plan_with_schema(
        &self,
        query: Query,
        planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        let set_expr = query.body;
        if let Some(with) = query.with {
            // Process CTEs from top to bottom
            // do not allow self-references
            // if with.recursive {
            //     return not_impl_err!("Recursive CTEs are not supported");
            // }

            for cte in with.cte_tables {
                // A `WITH` block can't use the same name more than once
                let cte_name = self.normalizer.normalize(cte.alias.name.clone());
                if planner_context.contains_cte(&cte_name) {
                    return sql_err!(ParserError(format!(
                        "WITH query name {cte_name:?} specified more than once"
                    )));
                }

                let logical_plan = if with.recursive {
                    self.recursive_query(cte.clone(), &mut planner_context.clone())?
                } else{
                // create logical plan & pass backreferencing CTEs
                // CTE expr don't need extend outer_query_schema
                    self.query_to_plan(*cte.query, &mut planner_context.clone())?
                };

                // Each `WITH` block can change the column names in the last
                // projection (e.g. "WITH table(t1, t2) AS SELECT 1, 2").
                let logical_plan = self.apply_table_alias(logical_plan, cte.alias)?;

                planner_context.insert_cte(cte_name, logical_plan);
            }
        }
        let plan = self.set_expr_to_plan(*(set_expr.clone()), planner_context)?;

        let plan = self.order_by(plan, query.order_by, planner_context)?;
        let plan = self.limit(plan, query.offset, query.limit)?;

        let plan = match *set_expr {
            SetExpr::Select(select) if select.into.is_some() => {
                let select_into = select.into.unwrap();
                LogicalPlan::Ddl(DdlStatement::CreateMemoryTable(CreateMemoryTable {
                    name: self.object_name_to_table_reference(select_into.name)?,
                    constraints: Constraints::empty(),
                    input: Arc::new(plan),
                    if_not_exists: false,
                    or_replace: false,
                }))
            }
            _ => plan,
        };

        Ok(plan)
    }

    fn recursive_query(&self, cte: Cte, planner_context: &mut PlannerContext) -> Result<LogicalPlan> {
        let (left_expr, right_expr, set_quantifier) = match *cte.query.body {
            SetExpr::SetOperation {
                op: SetOperator::Union,
                left,
                right,
                set_quantifier,
            } => (left, right, set_quantifier),
            _ => {
                return plan_err!("A recursive query must have a UNION as the outermost component");
            }
        };

        // The static part of the plan
        let plan = self.set_expr_to_plan(*left_expr, planner_context)?;

        let cte_name = self.normalizer.normalize(cte.alias.name.clone());

        let cte_worktable: Arc<dyn TableSource> = Arc::new(CteWorkTable::new(&cte_name, Arc::new(Schema::from(plan.schema().as_ref()))));

        let work_table_plan = LogicalPlanBuilder::scan(
            cte_name.to_string(),
            Arc::clone(&cte_worktable),
            None,
        )?
            .build()?;

        planner_context.insert_cte(cte_name.clone(), work_table_plan);

        let recursive_plan = self.set_expr_to_plan(*right_expr, planner_context)?;
        let distinct = !Self::is_union_all(set_quantifier)?;

        if distinct {
            return not_impl_err!("UNION is not supported in recursive CTEs (you must \
            use UNION ALL)");
        }

        LogicalPlanBuilder::from(plan)
            .to_recursive_query(cte_name, recursive_plan, distinct)?
            .build()
    }

    pub(super) fn is_union_all(set_quantifier: SetQuantifier) -> Result<bool> {
        match set_quantifier {
            SetQuantifier::All => Ok(true),
            SetQuantifier::Distinct | SetQuantifier::None => Ok(false),
            SetQuantifier::ByName => {
                not_impl_err!("UNION BY NAME not implemented")
            }
            SetQuantifier::AllByName => {
                not_impl_err!("UNION ALL BY NAME not implemented")
            }
        }
    }

    /// Wrap a plan in a limit
    fn limit(
        &self,
        input: LogicalPlan,
        skip: Option<SQLOffset>,
        fetch: Option<SQLExpr>,
    ) -> Result<LogicalPlan> {
        if skip.is_none() && fetch.is_none() {
            return Ok(input);
        }

        let skip = match skip {
            Some(skip_expr) => match self.sql_to_expr(
                skip_expr.value,
                input.schema(),
                &mut PlannerContext::new(),
            )? {
                Expr::Literal(ScalarValue::Int64(Some(s))) => {
                    if s < 0 {
                        return plan_err!("Offset must be >= 0, '{s}' was provided.");
                    }
                    Ok(s as usize)
                }
                _ => plan_err!("Unexpected expression in OFFSET clause"),
            }?,
            _ => 0,
        };

        let fetch = match fetch {
            Some(limit_expr)
                if limit_expr != sqlparser::ast::Expr::Value(Value::Null) =>
            {
                let n = match self.sql_to_expr(
                    limit_expr,
                    input.schema(),
                    &mut PlannerContext::new(),
                )? {
                    Expr::Literal(ScalarValue::Int64(Some(n))) if n >= 0 => {
                        Ok(n as usize)
                    }
                    _ => plan_err!("LIMIT must not be negative"),
                }?;
                Some(n)
            }
            _ => None,
        };

        LogicalPlanBuilder::from(input).limit(skip, fetch)?.build()
    }

    /// Wrap the logical in a sort
    fn order_by(
        &self,
        plan: LogicalPlan,
        order_by: Vec<OrderByExpr>,
        planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        if order_by.is_empty() {
            return Ok(plan);
        }

        // Handle DISTINCT ON situation- this is the only situation in which we want to logically
        // do our ordering before the DISTINCT, as the DISTINCT row that is chosen will be decided
        // by said ordering- if we did the DISTINCT first, the order wouldn't matter
        // So, we check if our last logical plans are Projection and Distinct, which means we had a
        // DISTINCT ON situation (todo: this is a bit hacky, not sure how else to do it)
        // If so, we move the sort before the distinct
        if let LogicalPlan::Projection(mut p) = plan.clone() {
            if let LogicalPlan::Distinct(mut d) = (*p.input).clone() {
                if let Some(on_expr) = d.on_expr.clone() {
                    let parent_plan = d.input;

                    let order_by_expressions = self.order_by_to_sort_expr(
                        &order_by,
                        parent_plan.schema(),
                        planner_context,
                    )?;

                    let alias_map = match (*parent_plan).clone() {
                        LogicalPlan::Projection(p) => extract_aliases(p.expr.as_slice()),
                        _ => unreachable!(),
                    };

                    let on_expr = on_expr
                        .into_iter()
                        .map(|e| {
                            normalize_col(
                                resolve_aliases_to_exprs(&e, &alias_map)?,
                                &parent_plan,
                            )
                        })
                        .collect::<Result<Vec<_>>>()?;
                    let mut order_by_expressions = order_by_expressions
                        .into_iter()
                        .map(|e| {
                            normalize_col(
                                resolve_aliases_to_exprs(&e, &alias_map)?,
                                &parent_plan,
                            )
                        })
                        .collect::<Result<Vec<_>>>()?;

                    // First, we need to ensure the ORDER BY expressions start with our ON expression
                    // This is because the ON expression is used to determine the distinct key
                    let on_expr_length = on_expr.len();
                    for (i, expr) in on_expr.into_iter().enumerate() {
                        let order_exp = order_by_expressions.get(i);
                        match order_exp {
                            None => {}
                            Some(o) => match o {
                                Expr::Sort(sort_expr) => {
                                    if *sort_expr.expr != expr {
                                        return Err(DataFusionError::Plan(format!(
                                            "ORDER BY expression must start with ON expression for DISTINCT ON"
                                        )));
                                    }
                                }
                                _ => {
                                    return Err(DataFusionError::Internal(format!(
                                        "Unexpected expression in ORDER BY clause"
                                    )));
                                }
                            },
                        }
                    }

                    order_by_expressions = order_by_expressions
                        .into_iter()
                        .skip(on_expr_length)
                        .collect();

                    // Next, We need to move the sort BEFORE the distinct
                    let sort_plan = LogicalPlanBuilder::from((*parent_plan).clone())
                        .sort(order_by_expressions.clone())?
                        .build()?;
                    d.input = Arc::new(sort_plan);
                    p.input = Arc::new(LogicalPlan::Distinct(d.clone()));
                    return Ok(LogicalPlan::Projection(p));
                }
            }
        }

        let order_by_expressions =
            self.order_by_to_sort_expr(&order_by, plan.schema(), planner_context)?;
        LogicalPlanBuilder::from(plan)
            .sort(order_by_expressions)?
            .build()
    }
}
