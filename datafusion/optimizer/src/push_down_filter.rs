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

//! Push Down Filter optimizer rule ensures that filters are applied as early as possible in the plan

use crate::optimizer::ApplyOrder;
use crate::utils::{conjunction, split_conjunction};
use crate::{utils, OptimizerConfig, OptimizerRule};
use datafusion_common::tree_node::{Transformed, TreeNode, VisitRecursion};
use datafusion_common::{
    internal_err, Column, DFSchema, DFSchemaRef, DataFusionError, Result,
};
use datafusion_expr::expr::Alias;
use datafusion_expr::{
    and,
    expr_rewriter::replace_col,
    logical_plan::{CrossJoin, Join, JoinType, LogicalPlan, TableScan, Union},
    or, BinaryExpr, Expr, Filter, Operator, TableProviderFilterPushDown,
};
use itertools::Itertools;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// Push Down Filter optimizer rule pushes filter clauses down the plan
/// # Introduction
/// A filter-commutative operation is an operation whose result of filter(op(data)) = op(filter(data)).
/// An example of a filter-commutative operation is a projection; a counter-example is `limit`.
///
/// The filter-commutative property is column-specific. An aggregate grouped by A on SUM(B)
/// can commute with a filter that depends on A only, but does not commute with a filter that depends
/// on SUM(B).
///
/// This optimizer commutes filters with filter-commutative operations to push the filters
/// the closest possible to the scans, re-writing the filter expressions by every
/// projection that changes the filter's expression.
///
/// Filter: b Gt Int64(10)
///     Projection: a AS b
///
/// is optimized to
///
/// Projection: a AS b
///     Filter: a Gt Int64(10)  <--- changed from b to a
///
/// This performs a single pass through the plan. When it passes through a filter, it stores that filter,
/// and when it reaches a node that does not commute with it, it adds the filter to that place.
/// When it passes through a projection, it re-writes the filter's expression taking into account that projection.
/// When multiple filters would have been written, it `AND` their expressions into a single expression.
#[derive(Default)]
pub struct PushDownFilter {}

// For a given JOIN logical plan, determine whether each side of the join is preserved.
// We say a join side is preserved if the join returns all or a subset of the rows from
// the relevant side, such that each row of the output table directly maps to a row of
// the preserved input table. If a table is not preserved, it can provide extra null rows.
// That is, there may be rows in the output table that don't directly map to a row in the
// input table.
//
// For example:
//   - In an inner join, both sides are preserved, because each row of the output
//     maps directly to a row from each side.
//   - In a left join, the left side is preserved and the right is not, because
//     there may be rows in the output that don't directly map to a row in the
//     right input (due to nulls filling where there is no match on the right).
//
// This is important because we can always push down post-join filters to a preserved
// side of the join, assuming the filter only references columns from that side. For the
// non-preserved side it can be more tricky.
//
// Returns a tuple of booleans - (left_preserved, right_preserved).

fn lr_is_preserved(plan: &LogicalPlan) -> Result<(bool, bool)> {
    match plan {
        LogicalPlan::Join(Join { join_type, .. }) => match join_type {
            JoinType::Inner => Ok((true, true)),
            JoinType::Left => Ok((true, false)),
            JoinType::Right => Ok((false, true)),
            JoinType::Full => Ok((false, false)),
            // No columns from the right side of the join can be referenced in output
            // predicates for semi/anti joins, so whether we specify t/f doesn't matter.
            JoinType::LeftSemi | JoinType::LeftAnti => Ok((true, false)),
            // No columns from the left side of the join can be referenced in output
            // predicates for semi/anti joins, so whether we specify t/f doesn't matter.
            JoinType::RightSemi | JoinType::RightAnti => Ok((false, true)),
        },
        LogicalPlan::CrossJoin(_) => Ok((true, true)),
        _ => internal_err!("lr_is_preserved only valid for JOIN nodes"),
    }
}

// For a given JOIN logical plan, determine whether each side of the join is preserved
// in terms on join filtering.
// Predicates from join filter can only be pushed to preserved join side.
fn on_lr_is_preserved(plan: &LogicalPlan) -> Result<(bool, bool)> {
    match plan {
        LogicalPlan::Join(Join { join_type, .. }) => match join_type {
            JoinType::Inner => Ok((true, true)),
            JoinType::Left => Ok((false, true)),
            JoinType::Right => Ok((true, false)),
            JoinType::Full => Ok((false, false)),
            JoinType::LeftSemi | JoinType::RightSemi => Ok((true, true)),
            JoinType::LeftAnti => Ok((false, true)),
            JoinType::RightAnti => Ok((true, false)),
        },
        LogicalPlan::CrossJoin(_) => {
            internal_err!("on_lr_is_preserved cannot be applied to CROSSJOIN nodes")
        }
        _ => internal_err!("on_lr_is_preserved only valid for JOIN nodes"),
    }
}

// Determine which predicates in state can be pushed down to a given side of a join.
// To determine this, we need to know the schema of the relevant join side and whether
// or not the side's rows are preserved when joining. If the side is not preserved, we
// do not push down anything. Otherwise we can push down predicates where all of the
// relevant columns are contained on the relevant join side's schema.
fn can_pushdown_join_predicate(predicate: &Expr, schema: &DFSchema) -> Result<bool> {
    let schema_columns = schema
        .fields()
        .iter()
        .flat_map(|f| {
            [
                f.qualified_column(),
                // we need to push down filter using unqualified column as well
                f.unqualified_column(),
            ]
        })
        .collect::<HashSet<_>>();
    let columns = predicate.to_columns()?;

    Ok(schema_columns
        .intersection(&columns)
        .collect::<HashSet<_>>()
        .len()
        == columns.len())
}

// Determine whether the predicate can evaluate as the join conditions
fn can_evaluate_as_join_condition(predicate: &Expr) -> Result<bool> {
    let mut is_evaluate = true;
    predicate.apply(&mut |expr| match expr {
        Expr::Column(_)
        | Expr::Literal(_)
        | Expr::Placeholder(_)
        | Expr::ScalarVariable(_, _) => Ok(VisitRecursion::Skip),
        Expr::Exists { .. }
        | Expr::InSubquery(_)
        | Expr::ScalarSubquery(_)
        | Expr::OuterReferenceColumn(_, _)
        | Expr::ScalarUDF(..) => {
            is_evaluate = false;
            Ok(VisitRecursion::Stop)
        }
        Expr::Alias(_)
        | Expr::BinaryExpr(_)
        | Expr::Like(_)
        | Expr::SimilarTo(_)
        | Expr::Not(_)
        | Expr::IsNotNull(_)
        | Expr::IsNull(_)
        | Expr::IsTrue(_)
        | Expr::IsFalse(_)
        | Expr::IsUnknown(_)
        | Expr::IsNotTrue(_)
        | Expr::IsNotFalse(_)
        | Expr::IsNotUnknown(_)
        | Expr::Negative(_)
        | Expr::GetIndexedField(_)
        | Expr::Between(_)
        | Expr::Case(_)
        | Expr::Cast(_)
        | Expr::TryCast(_)
        | Expr::ScalarFunction(..)
        | Expr::InList { .. } => Ok(VisitRecursion::Continue),
        Expr::Sort(_)
        | Expr::AggregateFunction(_)
        | Expr::WindowFunction(_)
        | Expr::AggregateUDF { .. }
        | Expr::Wildcard
        | Expr::QualifiedWildcard { .. }
        | Expr::GroupingSet(_) => internal_err!("Unsupported predicate type"),
    })?;
    Ok(is_evaluate)
}

// examine OR clause to see if any useful clauses can be extracted and push down.
// extract at least one qual from each sub clauses of OR clause, then form the quals
// to new OR clause as predicate.
//
// Filter: (a = c and a < 20) or (b = d and b > 10)
//     join/crossjoin:
//          TableScan: projection=[a, b]
//          TableScan: projection=[c, d]
//
// is optimized to
//
// Filter: (a = c and a < 20) or (b = d and b > 10)
//     join/crossjoin:
//          Filter: (a < 20) or (b > 10)
//              TableScan: projection=[a, b]
//          TableScan: projection=[c, d]
//
// In general, predicates of this form:
//
// (A AND B) OR (C AND D)
//
// will be transformed to
//
// ((A AND B) OR (C AND D)) AND (A OR C)
//
// OR
//
// ((A AND B) OR (C AND D)) AND ((A AND B) OR C)
//
// OR
//
// do nothing.
//
fn extract_or_clauses_for_join(
    filters: &[&Expr],
    schema: &DFSchema,
    preserved: bool,
) -> Vec<Expr> {
    if preserved {
        return vec![];
    }

    let schema_columns = schema
        .fields()
        .iter()
        .flat_map(|f| {
            [
                f.qualified_column(),
                // we need to push down filter using unqualified column as well
                f.unqualified_column(),
            ]
        })
        .collect::<HashSet<_>>();

    let mut exprs = vec![];
    for expr in filters.iter() {
        if let Expr::BinaryExpr(BinaryExpr {
            left,
            op: Operator::Or,
            right,
        }) = expr
        {
            let left_expr = extract_or_clause(left.as_ref(), &schema_columns);
            let right_expr = extract_or_clause(right.as_ref(), &schema_columns);

            // If nothing can be extracted from any sub clauses, do nothing for this OR clause.
            if let (Some(left_expr), Some(right_expr)) = (left_expr, right_expr) {
                exprs.push(or(left_expr, right_expr));
            }
        }
    }

    // new formed OR clauses and their column references
    exprs
}

// extract qual from OR sub-clause.
//
// A qual is extracted if it only contains set of column references in schema_columns.
//
// For AND clause, we extract from both sub-clauses, then make new AND clause by extracted
// clauses if both extracted; Otherwise, use the extracted clause from any sub-clauses or None.
//
// For OR clause, we extract from both sub-clauses, then make new OR clause by extracted clauses if both extracted;
// Otherwise, return None.
//
// For other clause, apply the rule above to extract clause.
fn extract_or_clause(expr: &Expr, schema_columns: &HashSet<Column>) -> Option<Expr> {
    let mut predicate = None;

    match expr {
        Expr::BinaryExpr(BinaryExpr {
            left: l_expr,
            op: Operator::Or,
            right: r_expr,
        }) => {
            let l_expr = extract_or_clause(l_expr, schema_columns);
            let r_expr = extract_or_clause(r_expr, schema_columns);

            if let (Some(l_expr), Some(r_expr)) = (l_expr, r_expr) {
                predicate = Some(or(l_expr, r_expr));
            }
        }
        Expr::BinaryExpr(BinaryExpr {
            left: l_expr,
            op: Operator::And,
            right: r_expr,
        }) => {
            let l_expr = extract_or_clause(l_expr, schema_columns);
            let r_expr = extract_or_clause(r_expr, schema_columns);

            match (l_expr, r_expr) {
                (Some(l_expr), Some(r_expr)) => {
                    predicate = Some(and(l_expr, r_expr));
                }
                (Some(l_expr), None) => {
                    predicate = Some(l_expr);
                }
                (None, Some(r_expr)) => {
                    predicate = Some(r_expr);
                }
                (None, None) => {
                    predicate = None;
                }
            }
        }
        _ => {
            let columns = expr.to_columns().ok().unwrap();

            if schema_columns
                .intersection(&columns)
                .collect::<HashSet<_>>()
                .len()
                == columns.len()
            {
                predicate = Some(expr.clone());
            }
        }
    }

    predicate
}

fn can_push_and_keep_unpreserved_predicate(predicate: &Expr) -> bool {
    return match predicate {
        Expr::BinaryExpr(b) if b.op == Operator::Eq => {
            return if let (Expr::Column(_), Expr::Literal(_)) = (&*b.left, &*b.right) {
                true
            } else if let (Expr::Literal(_), Expr::Column(_)) = (&*b.left, &*b.right) {
                true
            } else {
                false
            }
        }
        _ => false,
    };
}

fn push_predicates(
    predicate: &Expr,
    join_plan: &LogicalPlan,
    schema: &DFSchemaRef,
    all_preserved: bool,
    preserved: bool,
    kept: &mut bool,
    predicates_to_push: &mut Vec<Expr>,
) -> Result<()> {
    if can_pushdown_join_predicate(predicate, schema)? {
        if preserved {
            *kept = true;
            predicates_to_push.push(predicate.clone());
        } else if can_push_and_keep_unpreserved_predicate(predicate) {
            predicates_to_push.push(predicate.clone());
        }
    } else {
        if all_preserved
            || (preserved && !matches!(predicate, Expr::IsNull(_)))
            || can_push_and_keep_unpreserved_predicate(predicate)
        {
            let aliased_predicates =
                get_aliased_predicates(join_plan, &vec![predicate.clone()])?;

            for predicate in aliased_predicates {
                if can_pushdown_join_predicate(&predicate, schema)? {
                    predicates_to_push.push(predicate);
                }
            }
        }
    }
    Ok(())
}
// push down join/cross-join
fn push_down_all_join(
    mut predicates: Vec<Expr>,
    join_plan: &LogicalPlan,
    left: &LogicalPlan,
    right: &LogicalPlan,
    on_filter: Vec<Expr>,
) -> Result<LogicalPlan> {
    let on_filter_empty = on_filter.is_empty();
    let (left_preserved, right_preserved) = lr_is_preserved(join_plan)?;
    let all_preserved = left_preserved && right_preserved;
    // The predicates can be divided to three categories:
    // 1) can push through join to its children(left or right)
    // 2) can be converted to join conditions if the join type is Inner
    // 3) should be kept as filter conditions
    let mut left_push = vec![];
    let mut right_push = vec![];
    let mut keep_predicates = vec![];
    let mut join_conditions = vec![];

    let inner_join = match join_plan {
        LogicalPlan::Join(Join { join_type, .. }) => *join_type == JoinType::Inner,
        _ => false,
    };

    if inner_join && !on_filter_empty {
        predicates.extend(on_filter.clone());
    }

    for predicate in predicates {
        let mut kept = false;

        push_predicates(
            &predicate,
            join_plan,
            left.schema(),
            all_preserved,
            left_preserved,
            &mut kept,
            &mut left_push,
        )?;
        push_predicates(
            &predicate,
            join_plan,
            right.schema(),
            all_preserved,
            right_preserved,
            &mut kept,
            &mut right_push,
        )?;

        if !kept {
            if inner_join && can_evaluate_as_join_condition(&predicate)? {
                join_conditions.push(predicate);
            } else {
                keep_predicates.push(predicate);
            }
        }
    }

    if !on_filter_empty && !inner_join {
        let (on_left_preserved, on_right_preserved) = on_lr_is_preserved(join_plan)?;
        for on in on_filter {
            if on_left_preserved && can_pushdown_join_predicate(&on, left.schema())? {
                left_push.push(on)
            } else if on_right_preserved
                && can_pushdown_join_predicate(&on, right.schema())?
            {
                right_push.push(on)
            } else {
                join_conditions.push(on)
            }
        }
    }

    // Extract from OR clause, generate new predicates for both side of join if possible.
    // We only track the unpushable predicates above.
    let or_to_left = extract_or_clauses_for_join(
        &keep_predicates.iter().collect::<Vec<_>>(),
        left.schema(),
        false,
    );
    let or_to_right = extract_or_clauses_for_join(
        &keep_predicates.iter().collect::<Vec<_>>(),
        right.schema(),
        false,
    );
    let on_or_to_left = extract_or_clauses_for_join(
        &join_conditions.iter().collect::<Vec<_>>(),
        left.schema(),
        left_preserved,
    );
    let on_or_to_right = extract_or_clauses_for_join(
        &join_conditions.iter().collect::<Vec<_>>(),
        right.schema(),
        right_preserved,
    );

    left_push.extend(or_to_left);
    left_push.extend(on_or_to_left);
    right_push.extend(or_to_right);
    right_push.extend(on_or_to_right);
    let left = match conjunction(left_push) {
        Some(predicate) => {
            LogicalPlan::Filter(Filter::try_new(predicate, Arc::new(left.clone()))?)
        }
        None => left.clone(),
    };
    let right = match conjunction(right_push) {
        Some(predicate) => {
            LogicalPlan::Filter(Filter::try_new(predicate, Arc::new(right.clone()))?)
        }
        None => right.clone(),
    };
    // Create a new Join with the new `left` and `right`
    //
    // expressions() output for Join is a vector consisting of
    //   1. join keys - columns mentioned in ON clause
    //   2. optional predicate - in case join filter is not empty,
    //      it always will be the last element, otherwise result
    //      vector will contain only join keys (without additional
    //      element representing filter).
    let expr = join_plan.expressions();
    let mut new_exprs = if !on_filter_empty {
        expr[..expr.len() - 1].to_vec()
    } else {
        expr
    };
    if !join_conditions.is_empty() {
        new_exprs.push(join_conditions.into_iter().reduce(Expr::and).unwrap());
    }
    let plan = join_plan.with_new_exprs(new_exprs, &[left, right])?;

    if keep_predicates.is_empty() {
        Ok(plan)
    } else {
        // wrap the join on the filter whose predicates must be kept
        match conjunction(keep_predicates) {
            Some(predicate) => Ok(LogicalPlan::Filter(Filter::try_new(
                predicate,
                Arc::new(plan),
            )?)),
            None => Ok(plan),
        }
    }
}

fn get_equivalent_columns_to_replace(
    predicate_columns: Vec<Column>,
    join: &Join,
) -> Result<Vec<(bool, Column, Column)>> {
    let (left_preserved, right_preserved) =
        lr_is_preserved(&LogicalPlan::Join(join.clone()))?;

    // Only allow both side key is column.
    let join_col_keys = join
        .on
        .iter()
        .flat_map(|(l, r)| match (l.try_into_col(), r.try_into_col()) {
            (Ok(l_col), Ok(r_col)) => Some((l_col, r_col)),
            _ => None,
        })
        .collect::<Vec<_>>();

    let mut columns_to_replace = vec![];
    for (l, r) in join_col_keys.clone().into_iter() {
        for col in predicate_columns.clone() {
            if col == l {
                columns_to_replace.push((right_preserved, col, r.clone()));
            } else if col == r {
                columns_to_replace.push((left_preserved, col, l.clone()));
            }
        }
    }

    return Ok(columns_to_replace);
}

fn unpreserved_null_was_previously_added(predicate: Expr, col: &Expr) -> bool {
    match predicate {
        Expr::BinaryExpr(BinaryExpr {
            left: _left,
            op: Operator::Or,
            right,
        }) => {
            return match *right {
                Expr::IsNull(expr) => *expr == col.clone(),
                _ => false,
            }
        }
        _ => false,
    }
}

fn get_aliased_predicates(
    plan: &LogicalPlan,
    predicates: &Vec<Expr>,
) -> Result<Vec<Expr>> {
    let join = match plan {
        LogicalPlan::Join(j) => j,
        _ => {
            return {
                let mut aliased_predicates = vec![];
                for input in plan.inputs() {
                    aliased_predicates.extend(get_aliased_predicates(input, predicates)?);
                }
                Ok(aliased_predicates)
            }
        }
    };

    let left_aliased_predicates = get_aliased_predicates(&join.left, predicates)?;
    let right_aliased_predicates = get_aliased_predicates(&join.right, predicates)?;

    let mut all_predicates = vec![];
    all_predicates.extend(predicates.clone());
    all_predicates.extend(left_aliased_predicates.clone());
    all_predicates.extend(right_aliased_predicates.clone());

    let mut new_predicates = vec![];
    for predicate in all_predicates {
        for (preserved, col_to_replace, new_col) in get_equivalent_columns_to_replace(
            predicate.to_columns()?.into_iter().collect(),
            join,
        )? {
            let mut replace_columns = HashMap::new();
            replace_columns.insert(&col_to_replace, &new_col);
            let col_expr = Expr::Column(new_col.clone());
            let mut aliased_predicate = replace_col(predicate.clone(), &replace_columns)?;
            if !preserved
                && !unpreserved_null_was_previously_added(
                    aliased_predicate.clone(),
                    &col_expr,
                )
            {
                aliased_predicate = aliased_predicate.or(col_expr.is_null());
            }
            new_predicates.push(aliased_predicate);
        }
    }

    new_predicates.extend(left_aliased_predicates);
    new_predicates.extend(right_aliased_predicates);
    Ok(new_predicates.into_iter().unique().collect())
}

fn push_down_join(
    plan: &LogicalPlan,
    join: &Join,
    parent_predicate: Option<&Expr>,
) -> Result<Option<LogicalPlan>> {
    let predicates = match parent_predicate {
        Some(parent_predicate) => {
            utils::split_conjunction_owned(parent_predicate.clone())
        }
        None => vec![],
    };

    // Convert JOIN ON predicate to Predicates
    let on_filters = join
        .filter
        .as_ref()
        .map(|e| utils::split_conjunction_owned(e.clone()))
        .unwrap_or_else(Vec::new);

    Ok(Some(push_down_all_join(
        predicates,
        plan,
        &join.left,
        &join.right,
        on_filters,
    )?))
}

impl OptimizerRule for PushDownFilter {
    fn name(&self) -> &str {
        "push_down_filter"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }

    fn try_optimize(
        &self,
        plan: &LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Option<LogicalPlan>> {
        let filter = match plan {
            LogicalPlan::Filter(filter) => filter,
            // we also need to pushdown filter in Join.
            LogicalPlan::Join(join) => {
                return push_down_join(plan, join, None);
            }
            _ => return Ok(None),
        };

        let child_plan = filter.input.as_ref();
        let new_plan = match child_plan {
            LogicalPlan::Filter(child_filter) => {
                let parents_predicates = split_conjunction(&filter.predicate);
                let set: HashSet<&&Expr> = parents_predicates.iter().collect();

                let new_predicates = parents_predicates
                    .iter()
                    .chain(
                        split_conjunction(&child_filter.predicate)
                            .iter()
                            .filter(|e| !set.contains(e)),
                    )
                    .map(|e| (*e).clone())
                    .collect::<Vec<_>>();
                let new_predicate = conjunction(new_predicates).ok_or_else(|| {
                    DataFusionError::Plan("at least one expression exists".to_string())
                })?;
                let new_filter = LogicalPlan::Filter(Filter::try_new(
                    new_predicate,
                    child_filter.input.clone(),
                )?);
                self.try_optimize(&new_filter, _config)?
                    .unwrap_or(new_filter)
            }
            LogicalPlan::Repartition(_) | LogicalPlan::Sort(_) => {
                // commutable
                let new_filter =
                    plan.with_new_inputs(&[child_plan.inputs()[0].clone()])?;
                child_plan.with_new_inputs(&[new_filter])?
            }
            LogicalPlan::Distinct(d) if d.on_expr.is_none() => {
                // note that we check if on_expr is None as DISTINCT ON with on_expr is not commutable
                let new_filter =
                    plan.with_new_inputs(&[child_plan.inputs()[0].clone()])?;
                child_plan.with_new_inputs(&[new_filter])?
            }
            LogicalPlan::SubqueryAlias(subquery_alias) => {
                let mut replace_map = HashMap::new();
                for (i, field) in
                    subquery_alias.input.schema().fields().iter().enumerate()
                {
                    replace_map.insert(
                        subquery_alias
                            .schema
                            .fields()
                            .get(i)
                            .unwrap()
                            .qualified_name(),
                        Expr::Column(field.qualified_column()),
                    );
                }
                let new_predicate =
                    replace_cols_by_name(filter.predicate.clone(), &replace_map)?;
                let new_filter = LogicalPlan::Filter(Filter::try_new(
                    new_predicate,
                    subquery_alias.input.clone(),
                )?);
                child_plan.with_new_inputs(&[new_filter])?
            }
            LogicalPlan::Projection(projection) => {
                // A projection is filter-commutable, but re-writes all predicate expressions
                // collect projection.
                let replace_map = projection
                    .schema
                    .fields()
                    .iter()
                    .enumerate()
                    .map(|(i, field)| {
                        // strip alias, as they should not be part of filters
                        let expr = match &projection.expr[i] {
                            Expr::Alias(Alias { expr, .. }) => expr.as_ref().clone(),
                            expr => expr.clone(),
                        };

                        (field.qualified_name(), expr)
                    })
                    .collect::<HashMap<_, _>>();

                // re-write all filters based on this projection
                // E.g. in `Filter: b\n  Projection: a > 1 as b`, we can swap them, but the filter must be "a > 1"
                let new_filter = LogicalPlan::Filter(Filter::try_new(
                    replace_cols_by_name(filter.predicate.clone(), &replace_map)?,
                    projection.input.clone(),
                )?);

                child_plan.with_new_inputs(&[new_filter])?
            }
            LogicalPlan::Union(union) => {
                let mut inputs = Vec::with_capacity(union.inputs.len());
                for input in &union.inputs {
                    let mut replace_map = HashMap::new();
                    for (i, field) in input.schema().fields().iter().enumerate() {
                        replace_map.insert(
                            union.schema.fields().get(i).unwrap().qualified_name(),
                            Expr::Column(field.qualified_column()),
                        );
                    }

                    let push_predicate =
                        replace_cols_by_name(filter.predicate.clone(), &replace_map)?;
                    inputs.push(Arc::new(LogicalPlan::Filter(Filter::try_new(
                        push_predicate,
                        input.clone(),
                    )?)))
                }
                LogicalPlan::Union(Union {
                    inputs,
                    schema: plan.schema().clone(),
                })
            }
            LogicalPlan::Aggregate(agg) => {
                // We can push down Predicate which in groupby_expr.
                let group_expr_columns = agg
                    .group_expr
                    .iter()
                    .map(|e| Ok(Column::from_qualified_name(e.display_name()?)))
                    .collect::<Result<HashSet<_>>>()?;

                let predicates = utils::split_conjunction_owned(filter.predicate.clone());

                let mut keep_predicates = vec![];
                let mut push_predicates = vec![];
                for expr in predicates {
                    let cols = expr.to_columns()?;
                    if cols.iter().all(|c| group_expr_columns.contains(c)) {
                        push_predicates.push(expr);
                    } else {
                        keep_predicates.push(expr);
                    }
                }

                // As for plan Filter: Column(a+b) > 0 -- Agg: groupby:[Column(a)+Column(b)]
                // After push, we need to replace `a+b` with Column(a)+Column(b)
                // So we need create a replace_map, add {`a+b` --> Expr(Column(a)+Column(b))}
                let mut replace_map = HashMap::new();
                for expr in &agg.group_expr {
                    replace_map.insert(expr.display_name()?, expr.clone());
                }
                let replaced_push_predicates = push_predicates
                    .iter()
                    .map(|expr| replace_cols_by_name(expr.clone(), &replace_map))
                    .collect::<Result<Vec<_>>>()?;

                let child = match conjunction(replaced_push_predicates) {
                    Some(predicate) => LogicalPlan::Filter(Filter::try_new(
                        predicate,
                        Arc::new((*agg.input).clone()),
                    )?),
                    None => (*agg.input).clone(),
                };
                let new_agg = filter.input.with_new_inputs(&vec![child])?;
                match conjunction(keep_predicates) {
                    Some(predicate) => LogicalPlan::Filter(Filter::try_new(
                        predicate,
                        Arc::new(new_agg),
                    )?),
                    None => new_agg,
                }
            }
            LogicalPlan::Join(join) => {
                match push_down_join(&filter.input, join, Some(&filter.predicate))? {
                    Some(optimized_plan) => optimized_plan,
                    None => return Ok(None),
                }
            }
            LogicalPlan::CrossJoin(CrossJoin { left, right, .. }) => {
                let predicates = utils::split_conjunction_owned(filter.predicate.clone());
                push_down_all_join(predicates, &filter.input, left, right, vec![])?
            }
            LogicalPlan::TableScan(scan) => {
                let filter_predicates = split_conjunction(&filter.predicate);
                let results = scan
                    .source
                    .supports_filters_pushdown(filter_predicates.as_slice())?;
                let zip = filter_predicates.iter().zip(results);

                let new_scan_filters = zip
                    .clone()
                    .filter(|(_, res)| res != &TableProviderFilterPushDown::Unsupported)
                    .map(|(pred, _)| *pred);
                let new_scan_filters: Vec<Expr> = scan
                    .filters
                    .iter()
                    .chain(new_scan_filters)
                    .unique()
                    .cloned()
                    .collect();
                let new_predicate: Vec<Expr> = zip
                    .filter(|(_, res)| res != &TableProviderFilterPushDown::Exact)
                    .map(|(pred, _)| (*pred).clone())
                    .collect();

                let new_scan = LogicalPlan::TableScan(TableScan {
                    source: scan.source.clone(),
                    projection: scan.projection.clone(),
                    projected_schema: scan.projected_schema.clone(),
                    table_name: scan.table_name.clone(),
                    filters: new_scan_filters,
                    fetch: scan.fetch,
                });

                match conjunction(new_predicate) {
                    Some(predicate) => LogicalPlan::Filter(Filter::try_new(
                        predicate,
                        Arc::new(new_scan),
                    )?),
                    None => new_scan,
                }
            }
            LogicalPlan::Extension(extension_plan) => {
                let prevent_cols =
                    extension_plan.node.prevent_predicate_push_down_columns();

                let predicates = utils::split_conjunction_owned(filter.predicate.clone());

                let mut keep_predicates = vec![];
                let mut push_predicates = vec![];
                for expr in predicates {
                    let cols = expr.to_columns()?;
                    if cols.iter().any(|c| prevent_cols.contains(&c.name)) {
                        keep_predicates.push(expr);
                    } else {
                        push_predicates.push(expr);
                    }
                }

                let new_children = match conjunction(push_predicates) {
                    Some(predicate) => extension_plan
                        .node
                        .inputs()
                        .into_iter()
                        .map(|child| {
                            Ok(LogicalPlan::Filter(Filter::try_new(
                                predicate.clone(),
                                Arc::new(child.clone()),
                            )?))
                        })
                        .collect::<Result<Vec<_>>>()?,
                    None => extension_plan.node.inputs().into_iter().cloned().collect(),
                };
                // extension with new inputs.
                let new_extension = child_plan.with_new_inputs(&new_children)?;

                match conjunction(keep_predicates) {
                    Some(predicate) => LogicalPlan::Filter(Filter::try_new(
                        predicate,
                        Arc::new(new_extension),
                    )?),
                    None => new_extension,
                }
            }
            _ => return Ok(None),
        };

        Ok(Some(new_plan))
    }
}

impl PushDownFilter {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

/// replaces columns by its name on the projection.
pub fn replace_cols_by_name(
    e: Expr,
    replace_map: &HashMap<String, Expr>,
) -> Result<Expr> {
    e.transform_up(&|expr| {
        Ok(if let Expr::Column(c) = &expr {
            match replace_map.get(&c.flat_name().to_lowercase()) {
                Some(new_c) => Transformed::Yes(new_c.clone()),
                None => Transformed::No(expr),
            }
        } else {
            Transformed::No(expr)
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::optimizer::Optimizer;
    use crate::rewrite_disjunctive_predicate::RewriteDisjunctivePredicate;
    use crate::test::*;
    use crate::OptimizerContext;
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use async_trait::async_trait;
    use datafusion_common::{DFSchema, DFSchemaRef};
    use datafusion_expr::logical_plan::table_scan;
    use datafusion_expr::{
        and, col, in_list, in_subquery, lit, logical_plan::JoinType, or, sum, BinaryExpr,
        Expr, Extension, LogicalPlanBuilder, Operator, TableSource, TableType,
        UserDefinedLogicalNodeCore,
    };
    use std::fmt::{Debug, Formatter};
    use std::sync::Arc;

    fn assert_optimized_plan_eq(plan: &LogicalPlan, expected: &str) -> Result<()> {
        crate::test::assert_optimized_plan_eq(
            Arc::new(PushDownFilter::new()),
            plan,
            expected,
        )
    }

    fn assert_optimized_plan_eq_with_rewrite_predicate(
        plan: &LogicalPlan,
        expected: &str,
    ) -> Result<()> {
        let optimizer = Optimizer::with_rules(vec![
            Arc::new(RewriteDisjunctivePredicate::new()),
            Arc::new(PushDownFilter::new()),
        ]);
        let mut optimized_plan = optimizer
            .optimize_recursively(
                optimizer.rules.get(0).unwrap(),
                plan,
                &OptimizerContext::new(),
            )?
            .unwrap_or_else(|| plan.clone());
        optimized_plan = optimizer
            .optimize_recursively(
                optimizer.rules.get(1).unwrap(),
                &optimized_plan,
                &OptimizerContext::new(),
            )?
            .unwrap_or_else(|| plan.clone());
        let formatted_plan = format!("{optimized_plan:?}");
        assert_eq!(plan.schema(), optimized_plan.schema());
        assert_eq!(expected, formatted_plan);
        Ok(())
    }

    #[test]
    fn filter_before_projection() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a"), col("b")])?
            .filter(col("a").eq(lit(1i64)))?
            .build()?;
        // filter is before projection
        let expected = "\
            Projection: test.a, test.b\
            \n  Filter: test.a = Int64(1)\
            \n    TableScan: test";
        assert_optimized_plan_eq(&plan, expected)
    }

    #[test]
    fn filter_after_limit() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a"), col("b")])?
            .limit(0, Some(10))?
            .filter(col("a").eq(lit(1i64)))?
            .build()?;
        // filter is before single projection
        let expected = "\
            Filter: test.a = Int64(1)\
            \n  Limit: skip=0, fetch=10\
            \n    Projection: test.a, test.b\
            \n      TableScan: test";
        assert_optimized_plan_eq(&plan, expected)
    }

    #[test]
    fn filter_no_columns() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(lit(0i64).eq(lit(1i64)))?
            .build()?;
        let expected = "\
            Filter: Int64(0) = Int64(1)\
            \n  TableScan: test";
        assert_optimized_plan_eq(&plan, expected)
    }

    #[test]
    fn filter_jump_2_plans() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a"), col("b"), col("c")])?
            .project(vec![col("c"), col("b")])?
            .filter(col("a").eq(lit(1i64)))?
            .build()?;
        // filter is before double projection
        let expected = "\
            Projection: test.c, test.b\
            \n  Projection: test.a, test.b, test.c\
            \n    Filter: test.a = Int64(1)\
            \n      TableScan: test";
        assert_optimized_plan_eq(&plan, expected)
    }

    #[test]
    fn filter_move_agg() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![col("a")], vec![sum(col("b")).alias("total_salary")])?
            .filter(col("a").gt(lit(10i64)))?
            .build()?;
        // filter of key aggregation is commutative
        let expected = "\
            Aggregate: groupBy=[[test.a]], aggr=[[SUM(test.b) AS total_salary]]\
            \n  Filter: test.a > Int64(10)\
            \n    TableScan: test";
        assert_optimized_plan_eq(&plan, expected)
    }

    #[test]
    fn filter_complex_group_by() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![add(col("b"), col("a"))], vec![sum(col("a")), col("b")])?
            .filter(col("b").gt(lit(10i64)))?
            .build()?;
        let expected = "Filter: test.b > Int64(10)\
        \n  Aggregate: groupBy=[[test.b + test.a]], aggr=[[SUM(test.a), test.b]]\
        \n    TableScan: test";
        assert_optimized_plan_eq(&plan, expected)
    }

    #[test]
    fn push_agg_need_replace_expr() -> Result<()> {
        let plan = LogicalPlanBuilder::from(test_table_scan()?)
            .aggregate(vec![add(col("b"), col("a"))], vec![sum(col("a")), col("b")])?
            .filter(col("test.b + test.a").gt(lit(10i64)))?
            .build()?;
        let expected =
            "Aggregate: groupBy=[[test.b + test.a]], aggr=[[SUM(test.a), test.b]]\
        \n  Filter: test.b + test.a > Int64(10)\
        \n    TableScan: test";
        assert_optimized_plan_eq(&plan, expected)
    }

    #[test]
    fn filter_keep_agg() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![col("a")], vec![sum(col("b")).alias("b")])?
            .filter(col("b").gt(lit(10i64)))?
            .build()?;
        // filter of aggregate is after aggregation since they are non-commutative
        let expected = "\
            Filter: b > Int64(10)\
            \n  Aggregate: groupBy=[[test.a]], aggr=[[SUM(test.b) AS b]]\
            \n    TableScan: test";
        assert_optimized_plan_eq(&plan, expected)
    }

    /// verifies that a filter is pushed to before a projection, the filter expression is correctly re-written
    #[test]
    fn alias() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a").alias("b"), col("c")])?
            .filter(col("b").eq(lit(1i64)))?
            .build()?;
        // filter is before projection
        let expected = "\
            Projection: test.a AS b, test.c\
            \n  Filter: test.a = Int64(1)\
            \n    TableScan: test";
        assert_optimized_plan_eq(&plan, expected)
    }

    fn add(left: Expr, right: Expr) -> Expr {
        Expr::BinaryExpr(BinaryExpr::new(
            Box::new(left),
            Operator::Plus,
            Box::new(right),
        ))
    }

    fn multiply(left: Expr, right: Expr) -> Expr {
        Expr::BinaryExpr(BinaryExpr::new(
            Box::new(left),
            Operator::Multiply,
            Box::new(right),
        ))
    }

    /// verifies that a filter is pushed to before a projection with a complex expression, the filter expression is correctly re-written
    #[test]
    fn complex_expression() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![
                add(multiply(col("a"), lit(2)), col("c")).alias("b"),
                col("c"),
            ])?
            .filter(col("b").eq(lit(1i64)))?
            .build()?;

        // not part of the test, just good to know:
        assert_eq!(
            format!("{plan:?}"),
            "\
            Filter: b = Int64(1)\
            \n  Projection: test.a * Int32(2) + test.c AS b, test.c\
            \n    TableScan: test"
        );

        // filter is before projection
        let expected = "\
            Projection: test.a * Int32(2) + test.c AS b, test.c\
            \n  Filter: test.a * Int32(2) + test.c = Int64(1)\
            \n    TableScan: test";
        assert_optimized_plan_eq(&plan, expected)
    }

    /// verifies that when a filter is pushed to after 2 projections, the filter expression is correctly re-written
    #[test]
    fn complex_plan() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![
                add(multiply(col("a"), lit(2)), col("c")).alias("b"),
                col("c"),
            ])?
            // second projection where we rename columns, just to make it difficult
            .project(vec![multiply(col("b"), lit(3)).alias("a"), col("c")])?
            .filter(col("a").eq(lit(1i64)))?
            .build()?;

        // not part of the test, just good to know:
        assert_eq!(
            format!("{plan:?}"),
            "\
            Filter: a = Int64(1)\
            \n  Projection: b * Int32(3) AS a, test.c\
            \n    Projection: test.a * Int32(2) + test.c AS b, test.c\
            \n      TableScan: test"
        );

        // filter is before the projections
        let expected = "\
        Projection: b * Int32(3) AS a, test.c\
        \n  Projection: test.a * Int32(2) + test.c AS b, test.c\
        \n    Filter: (test.a * Int32(2) + test.c) * Int32(3) = Int64(1)\
        \n      TableScan: test";
        assert_optimized_plan_eq(&plan, expected)
    }

    #[derive(Debug, PartialEq, Eq, Hash)]
    struct NoopPlan {
        input: Vec<LogicalPlan>,
        schema: DFSchemaRef,
    }

    impl UserDefinedLogicalNodeCore for NoopPlan {
        fn name(&self) -> &str {
            "NoopPlan"
        }

        fn inputs(&self) -> Vec<&LogicalPlan> {
            self.input.iter().collect()
        }

        fn schema(&self) -> &DFSchemaRef {
            &self.schema
        }

        fn expressions(&self) -> Vec<Expr> {
            self.input
                .iter()
                .flat_map(|child| child.expressions())
                .collect()
        }

        fn prevent_predicate_push_down_columns(&self) -> HashSet<String> {
            HashSet::from_iter(vec!["c".to_string()])
        }

        fn fmt_for_explain(&self, f: &mut Formatter) -> std::fmt::Result {
            write!(f, "NoopPlan")
        }

        fn from_template(&self, _exprs: &[Expr], inputs: &[LogicalPlan]) -> Self {
            Self {
                input: inputs.to_vec(),
                schema: self.schema.clone(),
            }
        }
    }

    #[test]
    fn user_defined_plan() -> Result<()> {
        let table_scan = test_table_scan()?;

        let custom_plan = LogicalPlan::Extension(Extension {
            node: Arc::new(NoopPlan {
                input: vec![table_scan.clone()],
                schema: table_scan.schema().clone(),
            }),
        });
        let plan = LogicalPlanBuilder::from(custom_plan)
            .filter(col("a").eq(lit(1i64)))?
            .build()?;

        // Push filter below NoopPlan
        let expected = "\
            NoopPlan\
            \n  Filter: test.a = Int64(1)\
            \n    TableScan: test";
        assert_optimized_plan_eq(&plan, expected)?;

        let custom_plan = LogicalPlan::Extension(Extension {
            node: Arc::new(NoopPlan {
                input: vec![table_scan.clone()],
                schema: table_scan.schema().clone(),
            }),
        });
        let plan = LogicalPlanBuilder::from(custom_plan)
            .filter(col("a").eq(lit(1i64)).and(col("c").eq(lit(2i64))))?
            .build()?;

        // Push only predicate on `a` below NoopPlan
        let expected = "\
            Filter: test.c = Int64(2)\
            \n  NoopPlan\
            \n    Filter: test.a = Int64(1)\
            \n      TableScan: test";
        assert_optimized_plan_eq(&plan, expected)?;

        let custom_plan = LogicalPlan::Extension(Extension {
            node: Arc::new(NoopPlan {
                input: vec![table_scan.clone(), table_scan.clone()],
                schema: table_scan.schema().clone(),
            }),
        });
        let plan = LogicalPlanBuilder::from(custom_plan)
            .filter(col("a").eq(lit(1i64)))?
            .build()?;

        // Push filter below NoopPlan for each child branch
        let expected = "\
            NoopPlan\
            \n  Filter: test.a = Int64(1)\
            \n    TableScan: test\
            \n  Filter: test.a = Int64(1)\
            \n    TableScan: test";
        assert_optimized_plan_eq(&plan, expected)?;

        let custom_plan = LogicalPlan::Extension(Extension {
            node: Arc::new(NoopPlan {
                input: vec![table_scan.clone(), table_scan.clone()],
                schema: table_scan.schema().clone(),
            }),
        });
        let plan = LogicalPlanBuilder::from(custom_plan)
            .filter(col("a").eq(lit(1i64)).and(col("c").eq(lit(2i64))))?
            .build()?;

        // Push only predicate on `a` below NoopPlan
        let expected = "\
            Filter: test.c = Int64(2)\
            \n  NoopPlan\
            \n    Filter: test.a = Int64(1)\
            \n      TableScan: test\
            \n    Filter: test.a = Int64(1)\
            \n      TableScan: test";
        assert_optimized_plan_eq(&plan, expected)
    }

    /// verifies that when two filters apply after an aggregation that only allows one to be pushed, one is pushed
    /// and the other not.
    #[test]
    fn multi_filter() -> Result<()> {
        // the aggregation allows one filter to pass (b), and the other one to not pass (SUM(c))
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a").alias("b"), col("c")])?
            .aggregate(vec![col("b")], vec![sum(col("c"))])?
            .filter(col("b").gt(lit(10i64)))?
            .filter(col("SUM(test.c)").gt(lit(10i64)))?
            .build()?;

        // not part of the test, just good to know:
        assert_eq!(
            format!("{plan:?}"),
            "\
            Filter: SUM(test.c) > Int64(10)\
            \n  Filter: b > Int64(10)\
            \n    Aggregate: groupBy=[[b]], aggr=[[SUM(test.c)]]\
            \n      Projection: test.a AS b, test.c\
            \n        TableScan: test"
        );

        // filter is before the projections
        let expected = "\
        Filter: SUM(test.c) > Int64(10)\
        \n  Aggregate: groupBy=[[b]], aggr=[[SUM(test.c)]]\
        \n    Projection: test.a AS b, test.c\
        \n      Filter: test.a > Int64(10)\
        \n        TableScan: test";
        assert_optimized_plan_eq(&plan, expected)
    }

    /// verifies that when a filter with two predicates is applied after an aggregation that only allows one to be pushed, one is pushed
    /// and the other not.
    #[test]
    fn split_filter() -> Result<()> {
        // the aggregation allows one filter to pass (b), and the other one to not pass (SUM(c))
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a").alias("b"), col("c")])?
            .aggregate(vec![col("b")], vec![sum(col("c"))])?
            .filter(and(
                col("SUM(test.c)").gt(lit(10i64)),
                and(col("b").gt(lit(10i64)), col("SUM(test.c)").lt(lit(20i64))),
            ))?
            .build()?;

        // not part of the test, just good to know:
        assert_eq!(
            format!("{plan:?}"),
            "\
            Filter: SUM(test.c) > Int64(10) AND b > Int64(10) AND SUM(test.c) < Int64(20)\
            \n  Aggregate: groupBy=[[b]], aggr=[[SUM(test.c)]]\
            \n    Projection: test.a AS b, test.c\
            \n      TableScan: test"
        );

        // filter is before the projections
        let expected = "\
        Filter: SUM(test.c) > Int64(10) AND SUM(test.c) < Int64(20)\
        \n  Aggregate: groupBy=[[b]], aggr=[[SUM(test.c)]]\
        \n    Projection: test.a AS b, test.c\
        \n      Filter: test.a > Int64(10)\
        \n        TableScan: test";
        assert_optimized_plan_eq(&plan, expected)
    }

    /// verifies that when two limits are in place, we jump neither
    #[test]
    fn double_limit() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a"), col("b")])?
            .limit(0, Some(20))?
            .limit(0, Some(10))?
            .project(vec![col("a"), col("b")])?
            .filter(col("a").eq(lit(1i64)))?
            .build()?;
        // filter does not just any of the limits
        let expected = "\
            Projection: test.a, test.b\
            \n  Filter: test.a = Int64(1)\
            \n    Limit: skip=0, fetch=10\
            \n      Limit: skip=0, fetch=20\
            \n        Projection: test.a, test.b\
            \n          TableScan: test";
        assert_optimized_plan_eq(&plan, expected)
    }

    #[test]
    fn union_all() -> Result<()> {
        let table_scan = test_table_scan()?;
        let table_scan2 = test_table_scan_with_name("test2")?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .union(LogicalPlanBuilder::from(table_scan2).build()?)?
            .filter(col("a").eq(lit(1i64)))?
            .build()?;
        // filter appears below Union
        let expected = "Union\
        \n  Filter: test.a = Int64(1)\
        \n    TableScan: test\
        \n  Filter: test2.a = Int64(1)\
        \n    TableScan: test2";
        assert_optimized_plan_eq(&plan, expected)
    }

    #[test]
    fn union_all_on_projection() -> Result<()> {
        let table_scan = test_table_scan()?;
        let table = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a").alias("b")])?
            .alias("test2")?;

        let plan = table
            .clone()
            .union(table.build()?)?
            .filter(col("b").eq(lit(1i64)))?
            .build()?;

        // filter appears below Union
        let expected = "Union\n  SubqueryAlias: test2\
        \n    Projection: test.a AS b\
        \n      Filter: test.a = Int64(1)\
        \n        TableScan: test\
        \n  SubqueryAlias: test2\
        \n    Projection: test.a AS b\
        \n      Filter: test.a = Int64(1)\
        \n        TableScan: test";
        assert_optimized_plan_eq(&plan, expected)
    }

    #[test]
    fn test_union_different_schema() -> Result<()> {
        let left = LogicalPlanBuilder::from(test_table_scan()?)
            .project(vec![col("a"), col("b"), col("c")])?
            .build()?;

        let schema = Schema::new(vec![
            Field::new("d", DataType::UInt32, false),
            Field::new("e", DataType::UInt32, false),
            Field::new("f", DataType::UInt32, false),
        ]);
        let right = table_scan(Some("test1"), &schema, None)?
            .project(vec![col("d"), col("e"), col("f")])?
            .build()?;
        let filter = and(col("test.a").eq(lit(1)), col("test1.d").gt(lit(2)));
        let plan = LogicalPlanBuilder::from(left)
            .cross_join(right)?
            .project(vec![col("test.a"), col("test1.d")])?
            .filter(filter)?
            .build()?;

        let expected = "Projection: test.a, test1.d\
        \n  CrossJoin:\
        \n    Projection: test.a, test.b, test.c\
        \n      Filter: test.a = Int32(1)\
        \n        TableScan: test\
        \n    Projection: test1.d, test1.e, test1.f\
        \n      Filter: test1.d > Int32(2)\
        \n        TableScan: test1";

        assert_optimized_plan_eq(&plan, expected)
    }

    #[test]
    fn test_project_same_name_different_qualifier() -> Result<()> {
        let table_scan = test_table_scan()?;
        let left = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a"), col("b"), col("c")])?
            .build()?;
        let right_table_scan = test_table_scan_with_name("test1")?;
        let right = LogicalPlanBuilder::from(right_table_scan)
            .project(vec![col("a"), col("b"), col("c")])?
            .build()?;
        let filter = and(col("test.a").eq(lit(1)), col("test1.a").gt(lit(2)));
        let plan = LogicalPlanBuilder::from(left)
            .cross_join(right)?
            .project(vec![col("test.a"), col("test1.a")])?
            .filter(filter)?
            .build()?;

        let expected = "Projection: test.a, test1.a\
        \n  CrossJoin:\
        \n    Projection: test.a, test.b, test.c\
        \n      Filter: test.a = Int32(1)\
        \n        TableScan: test\
        \n    Projection: test1.a, test1.b, test1.c\
        \n      Filter: test1.a > Int32(2)\
        \n        TableScan: test1";
        assert_optimized_plan_eq(&plan, expected)
    }

    /// verifies that filters with the same columns are correctly placed
    #[test]
    fn filter_2_breaks_limits() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a")])?
            .filter(col("a").lt_eq(lit(1i64)))?
            .limit(0, Some(1))?
            .project(vec![col("a")])?
            .filter(col("a").gt_eq(lit(1i64)))?
            .build()?;
        // Should be able to move both filters below the projections

        // not part of the test
        assert_eq!(
            format!("{plan:?}"),
            "Filter: test.a >= Int64(1)\
             \n  Projection: test.a\
             \n    Limit: skip=0, fetch=1\
             \n      Filter: test.a <= Int64(1)\
             \n        Projection: test.a\
             \n          TableScan: test"
        );

        let expected = "\
        Projection: test.a\
        \n  Filter: test.a >= Int64(1)\
        \n    Limit: skip=0, fetch=1\
        \n      Projection: test.a\
        \n        Filter: test.a <= Int64(1)\
        \n          TableScan: test";

        assert_optimized_plan_eq(&plan, expected)
    }

    /// verifies that filters to be placed on the same depth are ANDed
    #[test]
    fn two_filters_on_same_depth() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .limit(0, Some(1))?
            .filter(col("a").lt_eq(lit(1i64)))?
            .filter(col("a").gt_eq(lit(1i64)))?
            .project(vec![col("a")])?
            .build()?;

        // not part of the test
        assert_eq!(
            format!("{plan:?}"),
            "Projection: test.a\
            \n  Filter: test.a >= Int64(1)\
            \n    Filter: test.a <= Int64(1)\
            \n      Limit: skip=0, fetch=1\
            \n        TableScan: test"
        );

        let expected = "\
        Projection: test.a\
        \n  Filter: test.a >= Int64(1) AND test.a <= Int64(1)\
        \n    Limit: skip=0, fetch=1\
        \n      TableScan: test";

        assert_optimized_plan_eq(&plan, expected)
    }

    /// verifies that filters on a plan with user nodes are not lost
    /// (ARROW-10547)
    #[test]
    fn filters_user_defined_node() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(col("a").lt_eq(lit(1i64)))?
            .build()?;

        let plan = user_defined::new(plan);

        let expected = "\
            TestUserDefined\
             \n  Filter: test.a <= Int64(1)\
             \n    TableScan: test";

        // not part of the test
        assert_eq!(format!("{plan:?}"), expected);

        assert_optimized_plan_eq(&plan, expected)
    }

    /// post-on-join predicates on a column common to both sides is pushed to both sides
    #[test]
    fn filter_on_join_on_common_independent() -> Result<()> {
        let table_scan = test_table_scan()?;
        let left = LogicalPlanBuilder::from(table_scan).build()?;
        let right_table_scan = test_table_scan_with_name("test2")?;
        let right = LogicalPlanBuilder::from(right_table_scan)
            .project(vec![col("a")])?
            .build()?;
        let plan = LogicalPlanBuilder::from(left)
            .join(
                right,
                JoinType::Inner,
                (vec![Column::from_name("a")], vec![Column::from_name("a")]),
                None,
            )?
            .filter(col("test.a").lt_eq(lit(1i64)))?
            .build()?;

        // not part of the test, just good to know:
        assert_eq!(
            format!("{plan:?}"),
            "Filter: test.a <= Int64(1)\
            \n  Inner Join: test.a = test2.a\
            \n    TableScan: test\
            \n    Projection: test2.a\
            \n      TableScan: test2"
        );

        // filter sent to side before the join
        let expected = "\
        Inner Join: test.a = test2.a\
        \n  Filter: test.a <= Int64(1)\
        \n    TableScan: test\
        \n  Projection: test2.a\
        \n    Filter: test2.a <= Int64(1)\
        \n      TableScan: test2";
        assert_optimized_plan_eq(&plan, expected)
    }

    /// post-using-join predicates on a column common to both sides is pushed to both sides
    #[test]
    fn filter_using_join_on_common_independent() -> Result<()> {
        let table_scan = test_table_scan()?;
        let left = LogicalPlanBuilder::from(table_scan).build()?;
        let right_table_scan = test_table_scan_with_name("test2")?;
        let right = LogicalPlanBuilder::from(right_table_scan)
            .project(vec![col("a")])?
            .build()?;
        let plan = LogicalPlanBuilder::from(left)
            .join_using(
                right,
                JoinType::Inner,
                vec![Column::from_name("a".to_string())],
            )?
            .filter(col("a").lt_eq(lit(1i64)))?
            .build()?;

        // not part of the test, just good to know:
        assert_eq!(
            format!("{plan:?}"),
            "Filter: test.a <= Int64(1)\
            \n  Inner Join: Using test.a = test2.a\
            \n    TableScan: test\
            \n    Projection: test2.a\
            \n      TableScan: test2"
        );

        // filter sent to side before the join
        let expected = "\
        Inner Join: Using test.a = test2.a\
        \n  Filter: test.a <= Int64(1)\
        \n    TableScan: test\
        \n  Projection: test2.a\
        \n    Filter: test2.a <= Int64(1)\
        \n      TableScan: test2";
        assert_optimized_plan_eq(&plan, expected)
    }

    /// post-join predicates with columns from both sides are converted to join filterss
    #[test]
    fn filter_join_on_common_dependent() -> Result<()> {
        let table_scan = test_table_scan()?;
        let left = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a"), col("c")])?
            .build()?;
        let right_table_scan = test_table_scan_with_name("test2")?;
        let right = LogicalPlanBuilder::from(right_table_scan)
            .project(vec![col("a"), col("b")])?
            .build()?;
        let plan = LogicalPlanBuilder::from(left)
            .join(
                right,
                JoinType::Inner,
                (vec![Column::from_name("a")], vec![Column::from_name("a")]),
                None,
            )?
            .filter(col("c").lt_eq(col("b")))?
            .build()?;

        // not part of the test, just good to know:
        assert_eq!(
            format!("{plan:?}"),
            "Filter: test.c <= test2.b\
            \n  Inner Join: test.a = test2.a\
            \n    Projection: test.a, test.c\
            \n      TableScan: test\
            \n    Projection: test2.a, test2.b\
            \n      TableScan: test2"
        );

        // Filter is converted to Join Filter
        let expected = "\
        Inner Join: test.a = test2.a Filter: test.c <= test2.b\
        \n  Projection: test.a, test.c\
        \n    TableScan: test\
        \n  Projection: test2.a, test2.b\
        \n    TableScan: test2";
        assert_optimized_plan_eq(&plan, expected)
    }

    /// post-join predicates with columns from one side of a join are pushed only to that side
    #[test]
    fn filter_join_on_one_side() -> Result<()> {
        let table_scan = test_table_scan()?;
        let left = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a"), col("b")])?
            .build()?;
        let table_scan_right = test_table_scan_with_name("test2")?;
        let right = LogicalPlanBuilder::from(table_scan_right)
            .project(vec![col("a"), col("c")])?
            .build()?;

        let plan = LogicalPlanBuilder::from(left)
            .join(
                right,
                JoinType::Inner,
                (vec![Column::from_name("a")], vec![Column::from_name("a")]),
                None,
            )?
            .filter(col("b").lt_eq(lit(1i64)))?
            .build()?;

        // not part of the test, just good to know:
        assert_eq!(
            format!("{plan:?}"),
            "Filter: test.b <= Int64(1)\
            \n  Inner Join: test.a = test2.a\
            \n    Projection: test.a, test.b\
            \n      TableScan: test\
            \n    Projection: test2.a, test2.c\
            \n      TableScan: test2"
        );

        let expected = "\
        Inner Join: test.a = test2.a\
        \n  Projection: test.a, test.b\
        \n    Filter: test.b <= Int64(1)\
        \n      TableScan: test\
        \n  Projection: test2.a, test2.c\
        \n    TableScan: test2";
        assert_optimized_plan_eq(&plan, expected)
    }

    /// post-join predicates on the right side of a left join are not duplicated
    /// TODO: In this case we can sometimes convert the join to an INNER join
    #[test]
    fn filter_using_left_join() -> Result<()> {
        let table_scan = test_table_scan()?;
        let left = LogicalPlanBuilder::from(table_scan).build()?;
        let right_table_scan = test_table_scan_with_name("test2")?;
        let right = LogicalPlanBuilder::from(right_table_scan)
            .project(vec![col("a")])?
            .build()?;
        let plan = LogicalPlanBuilder::from(left)
            .join_using(
                right,
                JoinType::Left,
                vec![Column::from_name("a".to_string())],
            )?
            .filter(col("test2.a").lt_eq(lit(1i64)))?
            .build()?;

        // not part of the test, just good to know:
        assert_eq!(
            format!("{plan:?}"),
            "Filter: test2.a <= Int64(1)\
            \n  Left Join: Using test.a = test2.a\
            \n    TableScan: test\
            \n    Projection: test2.a\
            \n      TableScan: test2"
        );

        // filter not duplicated nor pushed down - i.e. noop
        let expected = "\
        Filter: test2.a <= Int64(1)\
        \n  Left Join: Using test.a = test2.a\
        \n    TableScan: test\
        \n    Projection: test2.a\
        \n      TableScan: test2";
        assert_optimized_plan_eq(&plan, expected)
    }

    /// post-join predicates on the left side of a right join are not duplicated
    #[test]
    fn filter_using_right_join() -> Result<()> {
        let table_scan = test_table_scan()?;
        let left = LogicalPlanBuilder::from(table_scan).build()?;
        let right_table_scan = test_table_scan_with_name("test2")?;
        let right = LogicalPlanBuilder::from(right_table_scan)
            .project(vec![col("a")])?
            .build()?;
        let plan = LogicalPlanBuilder::from(left)
            .join_using(
                right,
                JoinType::Right,
                vec![Column::from_name("a".to_string())],
            )?
            .filter(col("test.a").lt_eq(lit(1i64)))?
            .build()?;

        // not part of the test, just good to know:
        assert_eq!(
            format!("{plan:?}"),
            "Filter: test.a <= Int64(1)\
            \n  Right Join: Using test.a = test2.a\
            \n    TableScan: test\
            \n    Projection: test2.a\
            \n      TableScan: test2"
        );

        // filter not duplicated nor pushed down - i.e. noop
        let expected = "\
        Filter: test.a <= Int64(1)\
        \n  Right Join: Using test.a = test2.a\
        \n    TableScan: test\
        \n    Projection: test2.a\
        \n      TableScan: test2";
        assert_optimized_plan_eq(&plan, expected)
    }

    /// post-left-join predicate on a column common to both sides is only pushed to the left side
    /// i.e. - not duplicated to the right side
    #[test]
    fn filter_using_left_join_on_common() -> Result<()> {
        let table_scan = test_table_scan()?;
        let left = LogicalPlanBuilder::from(table_scan).build()?;
        let right_table_scan = test_table_scan_with_name("test2")?;
        let right = LogicalPlanBuilder::from(right_table_scan)
            .project(vec![col("a")])?
            .build()?;
        let plan = LogicalPlanBuilder::from(left)
            .join_using(
                right,
                JoinType::Left,
                vec![Column::from_name("a".to_string())],
            )?
            .filter(col("a").lt_eq(lit(1i64)))?
            .build()?;

        // not part of the test, just good to know:
        assert_eq!(
            format!("{plan:?}"),
            "Filter: test.a <= Int64(1)\
            \n  Left Join: Using test.a = test2.a\
            \n    TableScan: test\
            \n    Projection: test2.a\
            \n      TableScan: test2"
        );

        // filter sent to left side of the join, not the right
        let expected = "\
        Left Join: Using test.a = test2.a\
        \n  Filter: test.a <= Int64(1)\
        \n    TableScan: test\
        \n  Projection: test2.a\
        \n    TableScan: test2";
        assert_optimized_plan_eq(&plan, expected)
    }

    /// post-right-join predicate on a column common to both sides is only pushed to the right side
    /// i.e. - not duplicated to the left side.
    #[test]
    fn filter_using_right_join_on_common() -> Result<()> {
        let table_scan = test_table_scan()?;
        let left = LogicalPlanBuilder::from(table_scan).build()?;
        let right_table_scan = test_table_scan_with_name("test2")?;
        let right = LogicalPlanBuilder::from(right_table_scan)
            .project(vec![col("a")])?
            .build()?;
        let plan = LogicalPlanBuilder::from(left)
            .join_using(
                right,
                JoinType::Right,
                vec![Column::from_name("a".to_string())],
            )?
            .filter(col("test2.a").lt_eq(lit(1i64)))?
            .build()?;

        // not part of the test, just good to know:
        assert_eq!(
            format!("{plan:?}"),
            "Filter: test2.a <= Int64(1)\
            \n  Right Join: Using test.a = test2.a\
            \n    TableScan: test\
            \n    Projection: test2.a\
            \n      TableScan: test2"
        );

        // filter sent to right side of join, not duplicated to the left
        let expected = "\
        Right Join: Using test.a = test2.a\
        \n  TableScan: test\
        \n  Projection: test2.a\
        \n    Filter: test2.a <= Int64(1)\
        \n      TableScan: test2";
        assert_optimized_plan_eq(&plan, expected)
    }

    /// single table predicate parts of ON condition should be pushed to both inputs
    #[test]
    fn join_on_with_filter() -> Result<()> {
        let table_scan = test_table_scan()?;
        let left = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a"), col("b"), col("c")])?
            .build()?;
        let right_table_scan = test_table_scan_with_name("test2")?;
        let right = LogicalPlanBuilder::from(right_table_scan)
            .project(vec![col("a"), col("b"), col("c")])?
            .build()?;
        let filter = col("test.c")
            .gt(lit(1u32))
            .and(col("test.b").lt(col("test2.b")))
            .and(col("test2.c").gt(lit(4u32)));
        let plan = LogicalPlanBuilder::from(left)
            .join(
                right,
                JoinType::Inner,
                (vec![Column::from_name("a")], vec![Column::from_name("a")]),
                Some(filter),
            )?
            .build()?;

        // not part of the test, just good to know:
        assert_eq!(
            format!("{plan:?}"),
            "Inner Join: test.a = test2.a Filter: test.c > UInt32(1) AND test.b < test2.b AND test2.c > UInt32(4)\
            \n  Projection: test.a, test.b, test.c\
            \n    TableScan: test\
            \n  Projection: test2.a, test2.b, test2.c\
            \n    TableScan: test2"
        );

        let expected = "\
        Inner Join: test.a = test2.a Filter: test.b < test2.b\
        \n  Projection: test.a, test.b, test.c\
        \n    Filter: test.c > UInt32(1)\
        \n      TableScan: test\
        \n  Projection: test2.a, test2.b, test2.c\
        \n    Filter: test2.c > UInt32(4)\
        \n      TableScan: test2";
        assert_optimized_plan_eq(&plan, expected)
    }

    /// join filter should be completely removed after pushdown
    #[test]
    fn join_filter_removed() -> Result<()> {
        let table_scan = test_table_scan()?;
        let left = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a"), col("b"), col("c")])?
            .build()?;
        let right_table_scan = test_table_scan_with_name("test2")?;
        let right = LogicalPlanBuilder::from(right_table_scan)
            .project(vec![col("a"), col("b"), col("c")])?
            .build()?;
        let filter = col("test.b")
            .gt(lit(1u32))
            .and(col("test2.c").gt(lit(4u32)));
        let plan = LogicalPlanBuilder::from(left)
            .join(
                right,
                JoinType::Inner,
                (vec![Column::from_name("a")], vec![Column::from_name("a")]),
                Some(filter),
            )?
            .build()?;

        // not part of the test, just good to know:
        assert_eq!(
            format!("{plan:?}"),
            "Inner Join: test.a = test2.a Filter: test.b > UInt32(1) AND test2.c > UInt32(4)\
            \n  Projection: test.a, test.b, test.c\
            \n    TableScan: test\
            \n  Projection: test2.a, test2.b, test2.c\
            \n    TableScan: test2"
        );

        let expected = "\
        Inner Join: test.a = test2.a\
        \n  Projection: test.a, test.b, test.c\
        \n    Filter: test.b > UInt32(1)\
        \n      TableScan: test\
        \n  Projection: test2.a, test2.b, test2.c\
        \n    Filter: test2.c > UInt32(4)\
        \n      TableScan: test2";
        assert_optimized_plan_eq(&plan, expected)
    }

    /// predicate on join key in filter expression should be pushed down to both inputs
    #[test]
    fn join_filter_on_common() -> Result<()> {
        let table_scan = test_table_scan()?;
        let left = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a")])?
            .build()?;
        let right_table_scan = test_table_scan_with_name("test2")?;
        let right = LogicalPlanBuilder::from(right_table_scan)
            .project(vec![col("b")])?
            .build()?;
        let filter = col("test.a").gt(lit(1u32));
        let plan = LogicalPlanBuilder::from(left)
            .join(
                right,
                JoinType::Inner,
                (vec![Column::from_name("a")], vec![Column::from_name("b")]),
                Some(filter),
            )?
            .build()?;

        // not part of the test, just good to know:
        assert_eq!(
            format!("{plan:?}"),
            "Inner Join: test.a = test2.b Filter: test.a > UInt32(1)\
            \n  Projection: test.a\
            \n    TableScan: test\
            \n  Projection: test2.b\
            \n    TableScan: test2"
        );

        let expected = "\
        Inner Join: test.a = test2.b\
        \n  Projection: test.a\
        \n    Filter: test.a > UInt32(1)\
        \n      TableScan: test\
        \n  Projection: test2.b\
        \n    Filter: test2.b > UInt32(1)\
        \n      TableScan: test2";
        assert_optimized_plan_eq(&plan, expected)
    }

    /// single table predicate parts of ON condition should be pushed to right input
    #[test]
    fn left_join_on_with_filter() -> Result<()> {
        let table_scan = test_table_scan()?;
        let left = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a"), col("b"), col("c")])?
            .build()?;
        let right_table_scan = test_table_scan_with_name("test2")?;
        let right = LogicalPlanBuilder::from(right_table_scan)
            .project(vec![col("a"), col("b"), col("c")])?
            .build()?;
        let filter = col("test.a")
            .gt(lit(1u32))
            .and(col("test.b").lt(col("test2.b")))
            .and(col("test2.c").gt(lit(4u32)));
        let plan = LogicalPlanBuilder::from(left)
            .join(
                right,
                JoinType::Left,
                (vec![Column::from_name("a")], vec![Column::from_name("a")]),
                Some(filter),
            )?
            .build()?;

        // not part of the test, just good to know:
        assert_eq!(
            format!("{plan:?}"),
            "Left Join: test.a = test2.a Filter: test.a > UInt32(1) AND test.b < test2.b AND test2.c > UInt32(4)\
            \n  Projection: test.a, test.b, test.c\
            \n    TableScan: test\
            \n  Projection: test2.a, test2.b, test2.c\
            \n    TableScan: test2"
        );

        let expected = "\
        Left Join: test.a = test2.a Filter: test.a > UInt32(1) AND test.b < test2.b\
        \n  Projection: test.a, test.b, test.c\
        \n    TableScan: test\
        \n  Projection: test2.a, test2.b, test2.c\
        \n    Filter: test2.c > UInt32(4)\
        \n      TableScan: test2";
        assert_optimized_plan_eq(&plan, expected)
    }

    /// single table predicate parts of ON condition should be pushed to left input
    #[test]
    fn right_join_on_with_filter() -> Result<()> {
        let table_scan = test_table_scan()?;
        let left = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a"), col("b"), col("c")])?
            .build()?;
        let right_table_scan = test_table_scan_with_name("test2")?;
        let right = LogicalPlanBuilder::from(right_table_scan)
            .project(vec![col("a"), col("b"), col("c")])?
            .build()?;
        let filter = col("test.a")
            .gt(lit(1u32))
            .and(col("test.b").lt(col("test2.b")))
            .and(col("test2.c").gt(lit(4u32)));
        let plan = LogicalPlanBuilder::from(left)
            .join(
                right,
                JoinType::Right,
                (vec![Column::from_name("a")], vec![Column::from_name("a")]),
                Some(filter),
            )?
            .build()?;

        // not part of the test, just good to know:
        assert_eq!(
            format!("{plan:?}"),
            "Right Join: test.a = test2.a Filter: test.a > UInt32(1) AND test.b < test2.b AND test2.c > UInt32(4)\
            \n  Projection: test.a, test.b, test.c\
            \n    TableScan: test\
            \n  Projection: test2.a, test2.b, test2.c\
            \n    TableScan: test2"
        );

        let expected = "\
        Right Join: test.a = test2.a Filter: test.b < test2.b AND test2.c > UInt32(4)\
        \n  Projection: test.a, test.b, test.c\
        \n    Filter: test.a > UInt32(1)\
        \n      TableScan: test\
        \n  Projection: test2.a, test2.b, test2.c\
        \n    TableScan: test2";
        assert_optimized_plan_eq(&plan, expected)
    }

    /// single table predicate parts of ON condition should not be pushed
    #[test]
    fn full_join_on_with_filter() -> Result<()> {
        let table_scan = test_table_scan()?;
        let left = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a"), col("b"), col("c")])?
            .build()?;
        let right_table_scan = test_table_scan_with_name("test2")?;
        let right = LogicalPlanBuilder::from(right_table_scan)
            .project(vec![col("a"), col("b"), col("c")])?
            .build()?;
        let filter = col("test.a")
            .gt(lit(1u32))
            .and(col("test.b").lt(col("test2.b")))
            .and(col("test2.c").gt(lit(4u32)));
        let plan = LogicalPlanBuilder::from(left)
            .join(
                right,
                JoinType::Full,
                (vec![Column::from_name("a")], vec![Column::from_name("a")]),
                Some(filter),
            )?
            .build()?;

        // not part of the test, just good to know:
        assert_eq!(
            format!("{plan:?}"),
            "Full Join: test.a = test2.a Filter: test.a > UInt32(1) AND test.b < test2.b AND test2.c > UInt32(4)\
            \n  Projection: test.a, test.b, test.c\
            \n    TableScan: test\
            \n  Projection: test2.a, test2.b, test2.c\
            \n    TableScan: test2"
        );

        let expected = &format!("{plan:?}");
        assert_optimized_plan_eq(&plan, expected)
    }

    struct PushDownProvider {
        pub filter_support: TableProviderFilterPushDown,
    }

    #[async_trait]
    impl TableSource for PushDownProvider {
        fn schema(&self) -> SchemaRef {
            Arc::new(Schema::new(vec![
                Field::new("a", DataType::Int32, true),
                Field::new("b", DataType::Int32, true),
            ]))
        }

        fn table_type(&self) -> TableType {
            TableType::Base
        }

        fn supports_filter_pushdown(
            &self,
            _e: &Expr,
        ) -> Result<TableProviderFilterPushDown> {
            Ok(self.filter_support.clone())
        }

        fn as_any(&self) -> &dyn std::any::Any {
            self
        }
    }

    fn table_scan_with_pushdown_provider(
        filter_support: TableProviderFilterPushDown,
    ) -> Result<LogicalPlan> {
        let test_provider = PushDownProvider { filter_support };

        let table_scan = LogicalPlan::TableScan(TableScan {
            table_name: "test".into(),
            filters: vec![],
            projected_schema: Arc::new(DFSchema::try_from(
                (*test_provider.schema()).clone(),
            )?),
            projection: None,
            source: Arc::new(test_provider),
            fetch: None,
        });

        LogicalPlanBuilder::from(table_scan)
            .filter(col("a").eq(lit(1i64)))?
            .build()
    }

    #[test]
    fn filter_with_table_provider_exact() -> Result<()> {
        let plan = table_scan_with_pushdown_provider(TableProviderFilterPushDown::Exact)?;

        let expected = "\
        TableScan: test, full_filters=[a = Int64(1)]";
        assert_optimized_plan_eq(&plan, expected)
    }

    #[test]
    fn filter_with_table_provider_inexact() -> Result<()> {
        let plan =
            table_scan_with_pushdown_provider(TableProviderFilterPushDown::Inexact)?;

        let expected = "\
        Filter: a = Int64(1)\
        \n  TableScan: test, partial_filters=[a = Int64(1)]";
        assert_optimized_plan_eq(&plan, expected)
    }

    #[test]
    fn filter_with_table_provider_multiple_invocations() -> Result<()> {
        let plan =
            table_scan_with_pushdown_provider(TableProviderFilterPushDown::Inexact)?;

        let optimised_plan = PushDownFilter::new()
            .try_optimize(&plan, &OptimizerContext::new())
            .expect("failed to optimize plan")
            .unwrap();

        let expected = "\
        Filter: a = Int64(1)\
        \n  TableScan: test, partial_filters=[a = Int64(1)]";

        // Optimizing the same plan multiple times should produce the same plan
        // each time.
        assert_optimized_plan_eq(&optimised_plan, expected)
    }

    #[test]
    fn filter_with_table_provider_unsupported() -> Result<()> {
        let plan =
            table_scan_with_pushdown_provider(TableProviderFilterPushDown::Unsupported)?;

        let expected = "\
        Filter: a = Int64(1)\
        \n  TableScan: test";
        assert_optimized_plan_eq(&plan, expected)
    }

    #[test]
    fn multi_combined_filter() -> Result<()> {
        let test_provider = PushDownProvider {
            filter_support: TableProviderFilterPushDown::Inexact,
        };

        let table_scan = LogicalPlan::TableScan(TableScan {
            table_name: "test".into(),
            filters: vec![col("a").eq(lit(10i64)), col("b").gt(lit(11i64))],
            projected_schema: Arc::new(DFSchema::try_from(
                (*test_provider.schema()).clone(),
            )?),
            projection: Some(vec![0]),
            source: Arc::new(test_provider),
            fetch: None,
        });

        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(and(col("a").eq(lit(10i64)), col("b").gt(lit(11i64))))?
            .project(vec![col("a"), col("b")])?
            .build()?;

        let expected = "Projection: a, b\
            \n  Filter: a = Int64(10) AND b > Int64(11)\
            \n    TableScan: test projection=[a], partial_filters=[a = Int64(10), b > Int64(11)]";

        assert_optimized_plan_eq(&plan, expected)
    }

    #[test]
    fn multi_combined_filter_exact() -> Result<()> {
        let test_provider = PushDownProvider {
            filter_support: TableProviderFilterPushDown::Exact,
        };

        let table_scan = LogicalPlan::TableScan(TableScan {
            table_name: "test".into(),
            filters: vec![],
            projected_schema: Arc::new(DFSchema::try_from(
                (*test_provider.schema()).clone(),
            )?),
            projection: Some(vec![0]),
            source: Arc::new(test_provider),
            fetch: None,
        });

        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(and(col("a").eq(lit(10i64)), col("b").gt(lit(11i64))))?
            .project(vec![col("a"), col("b")])?
            .build()?;

        let expected = r#"
Projection: a, b
  TableScan: test projection=[a], full_filters=[a = Int64(10), b > Int64(11)]
        "#
        .trim();

        assert_optimized_plan_eq(&plan, expected)
    }

    #[test]
    fn test_filter_with_alias() -> Result<()> {
        // in table scan the true col name is 'test.a',
        // but we rename it as 'b', and use col 'b' in filter
        // we need rewrite filter col before push down.
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a").alias("b"), col("c")])?
            .filter(and(col("b").gt(lit(10i64)), col("c").gt(lit(10i64))))?
            .build()?;

        // filter on col b
        assert_eq!(
            format!("{plan:?}"),
            "Filter: b > Int64(10) AND test.c > Int64(10)\
            \n  Projection: test.a AS b, test.c\
            \n    TableScan: test"
        );

        // rewrite filter col b to test.a
        let expected = "\
            Projection: test.a AS b, test.c\
            \n  Filter: test.a > Int64(10) AND test.c > Int64(10)\
            \n    TableScan: test\
            ";

        assert_optimized_plan_eq(&plan, expected)
    }

    #[test]
    fn test_filter_with_alias_2() -> Result<()> {
        // in table scan the true col name is 'test.a',
        // but we rename it as 'b', and use col 'b' in filter
        // we need rewrite filter col before push down.
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a").alias("b"), col("c")])?
            .project(vec![col("b"), col("c")])?
            .filter(and(col("b").gt(lit(10i64)), col("c").gt(lit(10i64))))?
            .build()?;

        // filter on col b
        assert_eq!(
            format!("{plan:?}"),
            "Filter: b > Int64(10) AND test.c > Int64(10)\
            \n  Projection: b, test.c\
            \n    Projection: test.a AS b, test.c\
            \n      TableScan: test\
            "
        );

        // rewrite filter col b to test.a
        let expected = "\
            Projection: b, test.c\
            \n  Projection: test.a AS b, test.c\
            \n    Filter: test.a > Int64(10) AND test.c > Int64(10)\
            \n      TableScan: test\
            ";

        assert_optimized_plan_eq(&plan, expected)
    }

    #[test]
    fn test_filter_with_multi_alias() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a").alias("b"), col("c").alias("d")])?
            .filter(and(col("b").gt(lit(10i64)), col("d").gt(lit(10i64))))?
            .build()?;

        // filter on col b and d
        assert_eq!(
            format!("{plan:?}"),
            "Filter: b > Int64(10) AND d > Int64(10)\
            \n  Projection: test.a AS b, test.c AS d\
            \n    TableScan: test\
            "
        );

        // rewrite filter col b to test.a, col d to test.c
        let expected = "\
            Projection: test.a AS b, test.c AS d\
            \n  Filter: test.a > Int64(10) AND test.c > Int64(10)\
            \n    TableScan: test\
            ";

        assert_optimized_plan_eq(&plan, expected)
    }

    /// predicate on join key in filter expression should be pushed down to both inputs
    #[test]
    fn join_filter_with_alias() -> Result<()> {
        let table_scan = test_table_scan()?;
        let left = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a").alias("c")])?
            .build()?;
        let right_table_scan = test_table_scan_with_name("test2")?;
        let right = LogicalPlanBuilder::from(right_table_scan)
            .project(vec![col("b").alias("d")])?
            .build()?;
        let filter = col("c").gt(lit(1u32));
        let plan = LogicalPlanBuilder::from(left)
            .join(
                right,
                JoinType::Inner,
                (vec![Column::from_name("c")], vec![Column::from_name("d")]),
                Some(filter),
            )?
            .build()?;

        assert_eq!(
            format!("{plan:?}"),
            "Inner Join: c = d Filter: c > UInt32(1)\
            \n  Projection: test.a AS c\
            \n    TableScan: test\
            \n  Projection: test2.b AS d\
            \n    TableScan: test2"
        );

        // Change filter on col `c`, 'd' to `test.a`, 'test.b'
        let expected = "\
        Inner Join: c = d\
        \n  Projection: test.a AS c\
        \n    Filter: test.a > UInt32(1)\
        \n      TableScan: test\
        \n  Projection: test2.b AS d\
        \n    Filter: test2.b > UInt32(1)\
        \n      TableScan: test2";
        assert_optimized_plan_eq(&plan, expected)
    }

    #[test]
    fn test_in_filter_with_alias() -> Result<()> {
        // in table scan the true col name is 'test.a',
        // but we rename it as 'b', and use col 'b' in filter
        // we need rewrite filter col before push down.
        let table_scan = test_table_scan()?;
        let filter_value = vec![lit(1u32), lit(2u32), lit(3u32), lit(4u32)];
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a").alias("b"), col("c")])?
            .filter(in_list(col("b"), filter_value, false))?
            .build()?;

        // filter on col b
        assert_eq!(
            format!("{plan:?}"),
            "Filter: b IN ([UInt32(1), UInt32(2), UInt32(3), UInt32(4)])\
            \n  Projection: test.a AS b, test.c\
            \n    TableScan: test\
            "
        );

        // rewrite filter col b to test.a
        let expected = "\
            Projection: test.a AS b, test.c\
            \n  Filter: test.a IN ([UInt32(1), UInt32(2), UInt32(3), UInt32(4)])\
            \n    TableScan: test\
            ";

        assert_optimized_plan_eq(&plan, expected)
    }

    #[test]
    fn test_in_filter_with_alias_2() -> Result<()> {
        // in table scan the true col name is 'test.a',
        // but we rename it as 'b', and use col 'b' in filter
        // we need rewrite filter col before push down.
        let table_scan = test_table_scan()?;
        let filter_value = vec![lit(1u32), lit(2u32), lit(3u32), lit(4u32)];
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a").alias("b"), col("c")])?
            .project(vec![col("b"), col("c")])?
            .filter(in_list(col("b"), filter_value, false))?
            .build()?;

        // filter on col b
        assert_eq!(
            format!("{plan:?}"),
            "Filter: b IN ([UInt32(1), UInt32(2), UInt32(3), UInt32(4)])\
            \n  Projection: b, test.c\
            \n    Projection: test.a AS b, test.c\
            \n      TableScan: test\
            "
        );

        // rewrite filter col b to test.a
        let expected = "\
            Projection: b, test.c\
            \n  Projection: test.a AS b, test.c\
            \n    Filter: test.a IN ([UInt32(1), UInt32(2), UInt32(3), UInt32(4)])\
            \n      TableScan: test\
            ";

        assert_optimized_plan_eq(&plan, expected)
    }

    #[test]
    fn test_in_subquery_with_alias() -> Result<()> {
        // in table scan the true col name is 'test.a',
        // but we rename it as 'b', and use col 'b' in subquery filter
        let table_scan = test_table_scan()?;
        let table_scan_sq = test_table_scan_with_name("sq")?;
        let subplan = Arc::new(
            LogicalPlanBuilder::from(table_scan_sq)
                .project(vec![col("c")])?
                .build()?,
        );
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a").alias("b"), col("c")])?
            .filter(in_subquery(col("b"), subplan))?
            .build()?;

        // filter on col b in subquery
        let expected_before = "\
        Filter: b IN (<subquery>)\
        \n  Subquery:\
        \n    Projection: sq.c\
        \n      TableScan: sq\
        \n  Projection: test.a AS b, test.c\
        \n    TableScan: test";
        assert_eq!(format!("{plan:?}"), expected_before);

        // rewrite filter col b to test.a
        let expected_after = "\
        Projection: test.a AS b, test.c\
        \n  Filter: test.a IN (<subquery>)\
        \n    Subquery:\
        \n      Projection: sq.c\
        \n        TableScan: sq\
        \n    TableScan: test";
        assert_optimized_plan_eq(&plan, expected_after)
    }

    #[test]
    fn test_propagation_of_optimized_inner_filters_with_projections() -> Result<()> {
        // SELECT a FROM (SELECT 1 AS a) b WHERE b.a = 1
        let plan = LogicalPlanBuilder::empty(true)
            .project(vec![lit(0i64).alias("a")])?
            .alias("b")?
            .project(vec![col("b.a")])?
            .alias("b")?
            .filter(col("b.a").eq(lit(1i64)))?
            .project(vec![col("b.a")])?
            .build()?;

        let expected_before = "Projection: b.a\
        \n  Filter: b.a = Int64(1)\
        \n    SubqueryAlias: b\
        \n      Projection: b.a\
        \n        SubqueryAlias: b\
        \n          Projection: Int64(0) AS a\
        \n            EmptyRelation";
        assert_eq!(format!("{plan:?}"), expected_before);

        // Ensure that the predicate without any columns (0 = 1) is
        // still there.
        let expected_after = "Projection: b.a\
        \n  SubqueryAlias: b\
        \n    Projection: b.a\
        \n      SubqueryAlias: b\
        \n        Projection: Int64(0) AS a\
        \n          Filter: Int64(0) = Int64(1)\
        \n            EmptyRelation";
        assert_optimized_plan_eq(&plan, expected_after)
    }

    #[test]
    fn test_crossjoin_with_or_clause() -> Result<()> {
        // select * from test,test1 where (test.a = test1.a and test.b > 1) or (test.b = test1.b and test.c < 10);
        let table_scan = test_table_scan()?;
        let left = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a"), col("b"), col("c")])?
            .build()?;
        let right_table_scan = test_table_scan_with_name("test1")?;
        let right = LogicalPlanBuilder::from(right_table_scan)
            .project(vec![col("a").alias("d"), col("a").alias("e")])?
            .build()?;
        let filter = or(
            and(col("a").eq(col("d")), col("b").gt(lit(1u32))),
            and(col("b").eq(col("e")), col("c").lt(lit(10u32))),
        );
        let plan = LogicalPlanBuilder::from(left)
            .cross_join(right)?
            .filter(filter)?
            .build()?;

        let expected = "\
        Filter: test.a = d AND test.b > UInt32(1) OR test.b = e AND test.c < UInt32(10)\
        \n  CrossJoin:\
        \n    Projection: test.a, test.b, test.c\
        \n      Filter: test.b > UInt32(1) OR test.c < UInt32(10)\
        \n        TableScan: test\
        \n    Projection: test1.a AS d, test1.a AS e\
        \n      TableScan: test1";
        assert_optimized_plan_eq_with_rewrite_predicate(&plan, expected)?;

        // Originally global state which can help to avoid duplicate Filters been generated and pushed down.
        // Now the global state is removed. Need to double confirm that avoid duplicate Filters.
        let optimized_plan = PushDownFilter::new()
            .try_optimize(&plan, &OptimizerContext::new())?
            .expect("failed to optimize plan");
        assert_optimized_plan_eq(&optimized_plan, expected)
    }

    #[test]
    fn left_semi_join_with_filters() -> Result<()> {
        let left = test_table_scan_with_name("test1")?;
        let right_table_scan = test_table_scan_with_name("test2")?;
        let right = LogicalPlanBuilder::from(right_table_scan)
            .project(vec![col("a"), col("b")])?
            .build()?;
        let plan = LogicalPlanBuilder::from(left)
            .join(
                right,
                JoinType::LeftSemi,
                (
                    vec![Column::from_qualified_name("test1.a")],
                    vec![Column::from_qualified_name("test2.a")],
                ),
                Some(
                    col("test1.b")
                        .gt(lit(1u32))
                        .and(col("test2.b").gt(lit(2u32))),
                ),
            )?
            .build()?;

        // not part of the test, just good to know:
        assert_eq!(
            format!("{plan:?}"),
            "LeftSemi Join: test1.a = test2.a Filter: test1.b > UInt32(1) AND test2.b > UInt32(2)\
            \n  TableScan: test1\
            \n  Projection: test2.a, test2.b\
            \n    TableScan: test2",
        );

        // Both side will be pushed down.
        let expected = "\
        LeftSemi Join: test1.a = test2.a\
        \n  Filter: test1.b > UInt32(1)\
        \n    TableScan: test1\
        \n  Projection: test2.a, test2.b\
        \n    Filter: test2.b > UInt32(2)\
        \n      TableScan: test2";
        assert_optimized_plan_eq(&plan, expected)
    }

    #[test]
    fn right_semi_join_with_filters() -> Result<()> {
        let left = test_table_scan_with_name("test1")?;
        let right_table_scan = test_table_scan_with_name("test2")?;
        let right = LogicalPlanBuilder::from(right_table_scan)
            .project(vec![col("a"), col("b")])?
            .build()?;
        let plan = LogicalPlanBuilder::from(left)
            .join(
                right,
                JoinType::RightSemi,
                (
                    vec![Column::from_qualified_name("test1.a")],
                    vec![Column::from_qualified_name("test2.a")],
                ),
                Some(
                    col("test1.b")
                        .gt(lit(1u32))
                        .and(col("test2.b").gt(lit(2u32))),
                ),
            )?
            .build()?;

        // not part of the test, just good to know:
        assert_eq!(
            format!("{plan:?}"),
            "RightSemi Join: test1.a = test2.a Filter: test1.b > UInt32(1) AND test2.b > UInt32(2)\
            \n  TableScan: test1\
            \n  Projection: test2.a, test2.b\
            \n    TableScan: test2",
        );

        // Both side will be pushed down.
        let expected = "\
        RightSemi Join: test1.a = test2.a\
        \n  Filter: test1.b > UInt32(1)\
        \n    TableScan: test1\
        \n  Projection: test2.a, test2.b\
        \n    Filter: test2.b > UInt32(2)\
        \n      TableScan: test2";
        assert_optimized_plan_eq(&plan, expected)
    }

    #[test]
    fn left_anti_join_with_filters() -> Result<()> {
        let table_scan = test_table_scan_with_name("test1")?;
        let left = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a"), col("b")])?
            .build()?;
        let right_table_scan = test_table_scan_with_name("test2")?;
        let right = LogicalPlanBuilder::from(right_table_scan)
            .project(vec![col("a"), col("b")])?
            .build()?;
        let plan = LogicalPlanBuilder::from(left)
            .join(
                right,
                JoinType::LeftAnti,
                (
                    vec![Column::from_qualified_name("test1.a")],
                    vec![Column::from_qualified_name("test2.a")],
                ),
                Some(
                    col("test1.b")
                        .gt(lit(1u32))
                        .and(col("test2.b").gt(lit(2u32))),
                ),
            )?
            .build()?;

        // not part of the test, just good to know:
        assert_eq!(
            format!("{plan:?}"),
            "LeftAnti Join: test1.a = test2.a Filter: test1.b > UInt32(1) AND test2.b > UInt32(2)\
            \n  Projection: test1.a, test1.b\
            \n    TableScan: test1\
            \n  Projection: test2.a, test2.b\
            \n    TableScan: test2",
        );

        // For left anti, filter of the right side filter can be pushed down.
        let expected = "\
        LeftAnti Join: test1.a = test2.a Filter: test1.b > UInt32(1)\
        \n  Projection: test1.a, test1.b\
        \n    TableScan: test1\
        \n  Projection: test2.a, test2.b\
        \n    Filter: test2.b > UInt32(2)\
        \n      TableScan: test2";
        assert_optimized_plan_eq(&plan, expected)
    }

    #[test]
    fn right_anti_join_with_filters() -> Result<()> {
        let table_scan = test_table_scan_with_name("test1")?;
        let left = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a"), col("b")])?
            .build()?;
        let right_table_scan = test_table_scan_with_name("test2")?;
        let right = LogicalPlanBuilder::from(right_table_scan)
            .project(vec![col("a"), col("b")])?
            .build()?;
        let plan = LogicalPlanBuilder::from(left)
            .join(
                right,
                JoinType::RightAnti,
                (
                    vec![Column::from_qualified_name("test1.a")],
                    vec![Column::from_qualified_name("test2.a")],
                ),
                Some(
                    col("test1.b")
                        .gt(lit(1u32))
                        .and(col("test2.b").gt(lit(2u32))),
                ),
            )?
            .build()?;

        // not part of the test, just good to know:
        assert_eq!(
            format!("{plan:?}"),
            "RightAnti Join: test1.a = test2.a Filter: test1.b > UInt32(1) AND test2.b > UInt32(2)\
            \n  Projection: test1.a, test1.b\
            \n    TableScan: test1\
            \n  Projection: test2.a, test2.b\
            \n    TableScan: test2",
        );

        // For right anti, filter of the left side can be pushed down.
        let expected = "RightAnti Join: test1.a = test2.a Filter: test2.b > UInt32(2)\
        \n  Projection: test1.a, test1.b\
        \n    Filter: test1.b > UInt32(1)\
        \n      TableScan: test1\
        \n  Projection: test2.a, test2.b\
        \n    TableScan: test2";
        assert_optimized_plan_eq(&plan, expected)
    }
}
