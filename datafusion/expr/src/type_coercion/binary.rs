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

//! Coercion rules for matching argument types for binary operators

use arrow::array::{new_empty_array, Array};
use arrow::compute::can_cast_types;
use arrow::datatypes::{
    DataType, Field, Fields, TimeUnit, DECIMAL128_MAX_PRECISION, DECIMAL128_MAX_SCALE,
    DECIMAL256_MAX_PRECISION, DECIMAL256_MAX_SCALE,
};

use datafusion_common::Result;
use datafusion_common::{plan_err, DataFusionError};

use crate::type_coercion::is_numeric;
use crate::Operator;

/// The type signature of an instantiation of binary expression
struct Signature {
    /// The type to coerce the left argument to
    lhs: DataType,
    /// The type to coerce the right argument to
    rhs: DataType,
    /// The return type of the expression
    ret: DataType,
}

impl Signature {
    /// A signature where the inputs are the same type as the output
    fn uniform(t: DataType) -> Self {
        Self {
            lhs: t.clone(),
            rhs: t.clone(),
            ret: t,
        }
    }

    /// A signature where the inputs are the same type with a boolean output
    fn comparison(t: DataType) -> Self {
        Self {
            lhs: t.clone(),
            rhs: t,
            ret: DataType::Boolean,
        }
    }
}

/// Returns a [`Signature`] for applying `op` to arguments of type `lhs` and `rhs`
fn signature(lhs: &DataType, op: &Operator, rhs: &DataType) -> Result<Signature> {
    match op {
        Operator::Eq
        | Operator::NotEq
        | Operator::Lt
        | Operator::LtEq
        | Operator::Gt
        | Operator::GtEq
        | Operator::IsDistinctFrom
        | Operator::IsNotDistinctFrom => comparison_coercion(lhs, rhs)
            .map(Signature::comparison)
            .ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "Cannot infer common argument type for comparison operation {lhs} {op} {rhs}"
                ))
            }),
        Operator::And | Operator::Or => match (lhs, rhs) {
            (DataType::Int64, DataType::Int64) |
            (DataType::Boolean, DataType::Int64) |
            (DataType::Int64, DataType::Boolean) |
            (DataType::Boolean, DataType::Boolean)
            | (DataType::Null, DataType::Null)
            | (DataType::Boolean, DataType::Null)
            | (DataType::Null, DataType::Boolean) => Ok(Signature::uniform(DataType::Boolean)),
            _ => plan_err!(
                "Cannot infer common argument type for logical boolean operation {lhs} {op} {rhs}"
            ),
        },
        Operator::RegexMatch
        | Operator::RegexIMatch
        | Operator::RegexNotMatch
        | Operator::RegexNotIMatch => regex_coercion(lhs, rhs)
            .map(Signature::comparison)
            .ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "Cannot infer common argument type for regex operation {lhs} {op} {rhs}"
                ))
            }),
        Operator::BitwiseAnd
        | Operator::BitwiseOr
        | Operator::BitwiseXor
        | Operator::BitwiseShiftRight
        | Operator::BitwiseShiftLeft => bitwise_coercion(lhs, rhs)
            .map(Signature::uniform)
            .ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "Cannot infer common type for bitwise operation {lhs} {op} {rhs}"
                ))
            }),
        Operator::StringConcat => string_concat_coercion(lhs, rhs)
            .map(Signature::uniform)
            .ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "Cannot infer common string type for string concat operation {lhs} {op} {rhs}"
                ))
            }),
        Operator::AtArrow if lhs == &json_type() && rhs == &json_type() => {
            Ok(Signature::comparison(lhs.clone()))
        }
        Operator::AtArrow | Operator::ArrowAt => array_coercion(lhs, rhs)
            .map(Signature::uniform)
            .ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "Cannot infer common array type for arrow operation {lhs} {op} {rhs}"
                ))
            }),
        Operator::ArrowAccess | Operator::LongArrow => Ok(Signature {
            lhs: lhs.clone(),
            rhs: rhs.clone(),
            ret: get_arrow_return_type(lhs, rhs, op)?,
        }),
        Operator::Plus
        | Operator::Minus
        | Operator::Multiply
        | Operator::Divide
        | Operator::Modulo => {
            let get_result = |lhs, rhs| {
                use arrow::compute::kernels::numeric::*;
                let l = new_empty_array(lhs);
                let r = new_empty_array(rhs);

                let result = match op {
                    Operator::Plus => add_wrapping(&l, &r),
                    Operator::Minus => sub_wrapping(&l, &r),
                    Operator::Multiply => mul_wrapping(&l, &r),
                    Operator::Divide => div(&l, &r),
                    Operator::Modulo => rem(&l, &r),
                    _ => unreachable!(),
                };
                result.map(|x| x.data_type().clone())
            };

            if let Ok(ret) = get_result(lhs, rhs) {
                // Temporal arithmetic, e.g. Date32 + Interval
                Ok(Signature {
                    lhs: lhs.clone(),
                    rhs: rhs.clone(),
                    ret,
                })
            } else if let Some(coerced) = temporal_coercion(lhs, rhs) {
                // Temporal arithmetic by first coercing to a common time representation
                // e.g. Date32 - Timestamp
                let ret = get_result(&coerced, &coerced).map_err(|e| {
                    DataFusionError::Plan(format!(
                        "Cannot get result type for temporal operation {coerced} {op} {coerced}: {e}"
                    ))
                })?;
                Ok(Signature {
                    lhs: coerced.clone(),
                    rhs: coerced,
                    ret,
                })
            } else if let Some((lhs, rhs)) = math_decimal_coercion(lhs, rhs) {
                // Decimal arithmetic, e.g. Decimal(10, 2) + Decimal(10, 0)
                let ret = get_result(&lhs, &rhs).map_err(|e| {
                    DataFusionError::Plan(format!(
                        "Cannot get result type for decimal operation {lhs} {op} {rhs}: {e}"
                    ))
                })?;
                Ok(Signature { lhs, rhs, ret })
            } else if let Some(numeric) = mathematics_numerical_coercion(lhs, rhs) {
                // Numeric arithmetic, e.g. Int32 + Int32
                Ok(Signature::uniform(numeric))
            } else if let Some(json_result_type) = json_coercion(lhs, rhs) {
                Ok(Signature {
                    lhs: lhs.clone(),
                    rhs: rhs.clone(),
                    ret: json_result_type,
                })
            } else {
                plan_err!("Cannot coerce arithmetic expression {lhs} {op} {rhs} to valid types")
            }
        }
    }
}

/// returns the resulting type of a binary expression evaluating the `op` with the left and right hand types
pub fn get_result_type(
    lhs: &DataType,
    op: &Operator,
    rhs: &DataType,
) -> Result<DataType> {
    signature(lhs, op, rhs).map(|sig| sig.ret)
}

/// Returns the coerced input types for a binary expression evaluating the `op` with the left and right hand types
pub fn get_input_types(
    lhs: &DataType,
    op: &Operator,
    rhs: &DataType,
) -> Result<(DataType, DataType)> {
    signature(lhs, op, rhs).map(|sig| (sig.lhs, sig.rhs))
}

/// Coercion rules for mathematics operators between decimal and non-decimal types.
fn math_decimal_coercion(
    lhs_type: &DataType,
    rhs_type: &DataType,
) -> Option<(DataType, DataType)> {
    use arrow::datatypes::DataType::*;

    match (lhs_type, rhs_type) {
        (Dictionary(_, value_type), _) => {
            let (value_type, rhs_type) = math_decimal_coercion(value_type, rhs_type)?;
            Some((value_type, rhs_type))
        }
        (_, Dictionary(_, value_type)) => {
            let (lhs_type, value_type) = math_decimal_coercion(lhs_type, value_type)?;
            Some((lhs_type, value_type))
        }
        (Null, dec_type @ Decimal128(_, _)) | (dec_type @ Decimal128(_, _), Null) => {
            Some((dec_type.clone(), dec_type.clone()))
        }
        (Decimal128(_, _), Decimal128(_, _)) => {
            Some((lhs_type.clone(), rhs_type.clone()))
        }
        // Unlike with comparison we don't coerce to a decimal in the case of floating point
        // numbers, instead falling back to floating point arithmetic instead
        (Decimal128(_, _), Int8 | Int16 | Int32 | Int64) => {
            Some((lhs_type.clone(), coerce_numeric_type_to_decimal(rhs_type)?))
        }
        (Int8 | Int16 | Int32 | Int64, Decimal128(_, _)) => {
            Some((coerce_numeric_type_to_decimal(lhs_type)?, rhs_type.clone()))
        }
        (Decimal256(_, _), Decimal256(_, _)) => {
            Some((lhs_type.clone(), rhs_type.clone()))
        }
        (Decimal256(_, _), Int8 | Int16 | Int32 | Int64) => Some((
            lhs_type.clone(),
            coerce_numeric_type_to_decimal256(rhs_type)?,
        )),
        (Int8 | Int16 | Int32 | Int64, Decimal256(_, _)) => Some((
            coerce_numeric_type_to_decimal256(lhs_type)?,
            rhs_type.clone(),
        )),
        _ => None,
    }
}

/// Returns the output type of applying bitwise operations such as
/// `&`, `|`, or `xor`to arguments of `lhs_type` and `rhs_type`.
fn bitwise_coercion(left_type: &DataType, right_type: &DataType) -> Option<DataType> {
    use arrow::datatypes::DataType::*;

    if !both_numeric_or_null_and_numeric(left_type, right_type) {
        return None;
    }

    if left_type == right_type {
        return Some(left_type.clone());
    }

    match (left_type, right_type) {
        (UInt64, _) | (_, UInt64) => Some(UInt64),
        (Int64, _)
        | (_, Int64)
        | (UInt32, Int8)
        | (Int8, UInt32)
        | (UInt32, Int16)
        | (Int16, UInt32)
        | (UInt32, Int32)
        | (Int32, UInt32) => Some(Int64),
        (Int32, _)
        | (_, Int32)
        | (UInt16, Int16)
        | (Int16, UInt16)
        | (UInt16, Int8)
        | (Int8, UInt16) => Some(Int32),
        (UInt32, _) | (_, UInt32) => Some(UInt32),
        (Int16, _) | (_, Int16) | (Int8, UInt8) | (UInt8, Int8) => Some(Int16),
        (UInt16, _) | (_, UInt16) => Some(UInt16),
        (Int8, _) | (_, Int8) => Some(Int8),
        (UInt8, _) | (_, UInt8) => Some(UInt8),
        _ => None,
    }
}

/// Coerce `lhs_type` and `rhs_type` to a common type for the purposes of a comparison operation
pub fn comparison_coercion(lhs_type: &DataType, rhs_type: &DataType) -> Option<DataType> {
    if lhs_type == rhs_type {
        // same type => equality is possible
        return Some(lhs_type.clone());
    }
    comparison_binary_numeric_coercion(lhs_type, rhs_type)
        .or_else(|| bool_numeric_coercision(lhs_type, rhs_type))
        .or_else(|| dictionary_coercion(lhs_type, rhs_type, true))
        .or_else(|| temporal_coercion(lhs_type, rhs_type))
        .or_else(|| string_coercion(lhs_type, rhs_type))
        .or_else(|| null_coercion(lhs_type, rhs_type))
        .or_else(|| string_numeric_coercion(lhs_type, rhs_type))
        .or_else(|| string_temporal_coercion(lhs_type, rhs_type))
        .or_else(|| binary_coercion(lhs_type, rhs_type))
}

fn bool_numeric_coercision(lhs_type: &DataType, rhs_type: &DataType) -> Option<DataType> {
    use arrow::datatypes::DataType::*;
    match (lhs_type, rhs_type) {
        (Boolean, _) if is_numeric(rhs_type) => Some(Boolean),
        (_, Boolean) if is_numeric(lhs_type) => Some(Boolean),
        _ => None,
    }
}

/// Coerce `lhs_type` and `rhs_type` to a common type for the purposes of a comparison operation
/// where one is numeric and one is `Utf8`/`LargeUtf8`.
fn string_numeric_coercion(lhs_type: &DataType, rhs_type: &DataType) -> Option<DataType> {
    use arrow::datatypes::DataType::*;
    match (lhs_type, rhs_type) {
        (Utf8, _) if is_numeric(rhs_type) => Some(Utf8),
        (LargeUtf8, _) if is_numeric(rhs_type) => Some(LargeUtf8),
        (_, Utf8) if is_numeric(lhs_type) => Some(Utf8),
        (_, LargeUtf8) if is_numeric(lhs_type) => Some(LargeUtf8),
        _ => None,
    }
}

/// Coerce `lhs_type` and `rhs_type` to a common type for the purposes of a comparison operation
/// where one is temporal and one is `Utf8`/`LargeUtf8`.
///
/// Note this cannot be performed in case of arithmetic as there is insufficient information
/// to correctly determine the type of argument. Consider
///
/// ```sql
/// timestamp > now() - '1 month'
/// interval > now() - '1970-01-2021'
/// ```
///
/// In the absence of a full type inference system, we can't determine the correct type
/// to parse the string argument
fn string_temporal_coercion(
    lhs_type: &DataType,
    rhs_type: &DataType,
) -> Option<DataType> {
    use arrow::datatypes::DataType::*;
    match (lhs_type, rhs_type) {
        (Utf8, Date32) | (Date32, Utf8) => Some(Date32),
        (Utf8, Date64) | (Date64, Utf8) => Some(Date64),
        (Utf8, Time32(unit)) | (Time32(unit), Utf8) => {
            match is_time_with_valid_unit(Time32(unit.clone())) {
                false => None,
                true => Some(Time32(unit.clone())),
            }
        }
        (Utf8, Time64(unit)) | (Time64(unit), Utf8) => {
            match is_time_with_valid_unit(Time64(unit.clone())) {
                false => None,
                true => Some(Time64(unit.clone())),
            }
        }
        (Timestamp(_, tz), Utf8) | (Utf8, Timestamp(_, tz)) => {
            Some(Timestamp(TimeUnit::Nanosecond, tz.clone()))
        }
        _ => None,
    }
}

/// Coerce `lhs_type` and `rhs_type` to a common type for the purposes of a comparison operation
/// where one both are numeric
fn comparison_binary_numeric_coercion(
    lhs_type: &DataType,
    rhs_type: &DataType,
) -> Option<DataType> {
    use arrow::datatypes::DataType::*;
    if !is_numeric(lhs_type) || !is_numeric(rhs_type) {
        return None;
    };

    // same type => all good
    if lhs_type == rhs_type {
        return Some(lhs_type.clone());
    }

    // these are ordered from most informative to least informative so
    // that the coercion does not lose information via truncation
    match (lhs_type, rhs_type) {
        // Prefer decimal data type over floating point for comparison operation
        (Decimal128(_, _), Decimal128(_, _)) => {
            get_wider_decimal_type(lhs_type, rhs_type)
        }
        (Decimal128(_, _), _) => get_comparison_common_decimal_type(lhs_type, rhs_type),
        (_, Decimal128(_, _)) => get_comparison_common_decimal_type(rhs_type, lhs_type),
        (Decimal256(_, _), Decimal256(_, _)) => {
            get_wider_decimal_type(lhs_type, rhs_type)
        }
        (Decimal256(_, _), _) => get_comparison_common_decimal_type(lhs_type, rhs_type),
        (_, Decimal256(_, _)) => get_comparison_common_decimal_type(rhs_type, lhs_type),
        (Float64, _) | (_, Float64) => Some(Float64),
        (_, Float32) | (Float32, _) => Some(Float32),
        // The following match arms encode the following logic: Given the two
        // integral types, we choose the narrowest possible integral type that
        // accommodates all values of both types. Note that some information
        // loss is inevitable when we have a signed type and a `UInt64`, in
        // which case we use `Int64`;i.e. the widest signed integral type.
        (Int64, _)
        | (_, Int64)
        | (UInt64, Int8)
        | (Int8, UInt64)
        | (UInt64, Int16)
        | (Int16, UInt64)
        | (UInt64, Int32)
        | (Int32, UInt64)
        | (UInt32, Int8)
        | (Int8, UInt32)
        | (UInt32, Int16)
        | (Int16, UInt32)
        | (UInt32, Int32)
        | (Int32, UInt32) => Some(Int64),
        (UInt64, _) | (_, UInt64) => Some(UInt64),
        (Int32, _)
        | (_, Int32)
        | (UInt16, Int16)
        | (Int16, UInt16)
        | (UInt16, Int8)
        | (Int8, UInt16) => Some(Int32),
        (UInt32, _) | (_, UInt32) => Some(UInt32),
        (Int16, _) | (_, Int16) | (Int8, UInt8) | (UInt8, Int8) => Some(Int16),
        (UInt16, _) | (_, UInt16) => Some(UInt16),
        (Int8, _) | (_, Int8) => Some(Int8),
        (UInt8, _) | (_, UInt8) => Some(UInt8),
        _ => None,
    }
}

/// Coerce `lhs_type` and `rhs_type` to a common type for the purposes of
/// a comparison operation where one is a decimal
fn get_comparison_common_decimal_type(
    decimal_type: &DataType,
    other_type: &DataType,
) -> Option<DataType> {
    use arrow::datatypes::DataType::*;
    match decimal_type {
        Decimal128(_, _) => {
            let other_decimal_type = coerce_numeric_type_to_decimal(other_type)?;
            get_wider_decimal_type(decimal_type, &other_decimal_type)
        }
        Decimal256(_, _) => {
            let other_decimal_type = coerce_numeric_type_to_decimal256(other_type)?;
            get_wider_decimal_type(decimal_type, &other_decimal_type)
        }
        _ => None,
    }
}

/// Returns a `DataType::Decimal128` that can store any value from either
/// `lhs_decimal_type` and `rhs_decimal_type`
///
/// The result decimal type is `(max(s1, s2) + max(p1-s1, p2-s2), max(s1, s2))`.
fn get_wider_decimal_type(
    lhs_decimal_type: &DataType,
    rhs_type: &DataType,
) -> Option<DataType> {
    match (lhs_decimal_type, rhs_type) {
        (DataType::Decimal128(p1, s1), DataType::Decimal128(p2, s2)) => {
            // max(s1, s2) + max(p1-s1, p2-s2), max(s1, s2)
            let s = *s1.max(s2);
            let range = (*p1 as i8 - s1).max(*p2 as i8 - s2);
            Some(create_decimal_type((range + s) as u8, s))
        }
        (DataType::Decimal256(p1, s1), DataType::Decimal256(p2, s2)) => {
            // max(s1, s2) + max(p1-s1, p2-s2), max(s1, s2)
            let s = *s1.max(s2);
            let range = (*p1 as i8 - s1).max(*p2 as i8 - s2);
            Some(create_decimal256_type((range + s) as u8, s))
        }
        (_, _) => None,
    }
}

/// Convert the numeric data type to the decimal data type.
/// Now, we just support the signed integer type and floating-point type.
fn coerce_numeric_type_to_decimal(numeric_type: &DataType) -> Option<DataType> {
    use arrow::datatypes::DataType::*;
    // This conversion rule is from spark
    // https://github.com/apache/spark/blob/1c81ad20296d34f137238dadd67cc6ae405944eb/sql/catalyst/src/main/scala/org/apache/spark/sql/types/DecimalType.scala#L127
    match numeric_type {
        Int8 => Some(Decimal128(3, 0)),
        Int16 => Some(Decimal128(5, 0)),
        Int32 => Some(Decimal128(10, 0)),
        Int64 => Some(Decimal128(20, 0)),
        // TODO if we convert the floating-point data to the decimal type, it maybe overflow.
        Float32 => Some(Decimal128(14, 7)),
        Float64 => Some(Decimal128(30, 15)),
        _ => None,
    }
}

/// Convert the numeric data type to the decimal data type.
/// Now, we just support the signed integer type and floating-point type.
fn coerce_numeric_type_to_decimal256(numeric_type: &DataType) -> Option<DataType> {
    use arrow::datatypes::DataType::*;
    // This conversion rule is from spark
    // https://github.com/apache/spark/blob/1c81ad20296d34f137238dadd67cc6ae405944eb/sql/catalyst/src/main/scala/org/apache/spark/sql/types/DecimalType.scala#L127
    match numeric_type {
        Int8 => Some(Decimal256(3, 0)),
        Int16 => Some(Decimal256(5, 0)),
        Int32 => Some(Decimal256(10, 0)),
        Int64 => Some(Decimal256(20, 0)),
        // TODO if we convert the floating-point data to the decimal type, it maybe overflow.
        Float32 => Some(Decimal256(14, 7)),
        Float64 => Some(Decimal256(30, 15)),
        _ => None,
    }
}

/// Returns the output type of applying mathematics operations such as
/// `+` to arguments of `lhs_type` and `rhs_type`.
fn mathematics_numerical_coercion(
    lhs_type: &DataType,
    rhs_type: &DataType,
) -> Option<DataType> {
    use arrow::datatypes::DataType::*;

    // error on any non-numeric type
    if !both_numeric_or_null_and_numeric(lhs_type, rhs_type) {
        return None;
    };

    // these are ordered from most informative to least informative so
    // that the coercion removes the least amount of information
    match (lhs_type, rhs_type) {
        (Dictionary(_, lhs_value_type), Dictionary(_, rhs_value_type)) => {
            mathematics_numerical_coercion(lhs_value_type, rhs_value_type)
        }
        (Dictionary(_, value_type), _) => {
            mathematics_numerical_coercion(value_type, rhs_type)
        }
        (_, Dictionary(_, value_type)) => {
            mathematics_numerical_coercion(lhs_type, value_type)
        }
        (Float64, _) | (_, Float64) => Some(Float64),
        (_, Float32) | (Float32, _) => Some(Float32),
        (Int64, _) | (_, Int64) => Some(Int64),
        (Int32, _) | (_, Int32) => Some(Int32),
        (Int16, _) | (_, Int16) => Some(Int16),
        (Int8, _) | (_, Int8) => Some(Int8),
        (UInt64, _) | (_, UInt64) => Some(UInt64),
        (UInt32, _) | (_, UInt32) => Some(UInt32),
        (UInt16, _) | (_, UInt16) => Some(UInt16),
        (UInt8, _) | (_, UInt8) => Some(UInt8),
        _ => None,
    }
}

/// The JSONB type in postgres supports some binary operators, like "-" to delete a key from an object.
/// This coercion is for these kind of operators
fn json_coercion(lhs_type: &DataType, rhs_type: &DataType) -> Option<DataType> {
    if lhs_type != &json_type() {
        return None;
    }
    match rhs_type {
        DataType::Utf8 | DataType::LargeUtf8 => Some(json_type()),
        _ => None,
    }
}

fn create_decimal_type(precision: u8, scale: i8) -> DataType {
    DataType::Decimal128(
        DECIMAL128_MAX_PRECISION.min(precision),
        DECIMAL128_MAX_SCALE.min(scale),
    )
}

fn create_decimal256_type(precision: u8, scale: i8) -> DataType {
    DataType::Decimal256(
        DECIMAL256_MAX_PRECISION.min(precision),
        DECIMAL256_MAX_SCALE.min(scale),
    )
}

/// Determine if at least of one of lhs and rhs is numeric, and the other must be NULL or numeric
fn both_numeric_or_null_and_numeric(lhs_type: &DataType, rhs_type: &DataType) -> bool {
    use arrow::datatypes::DataType::*;
    match (lhs_type, rhs_type) {
        (_, Null) => is_numeric(lhs_type),
        (Null, _) => is_numeric(rhs_type),
        (Dictionary(_, lhs_value_type), Dictionary(_, rhs_value_type)) => {
            is_numeric(lhs_value_type) && is_numeric(rhs_value_type)
        }
        (Dictionary(_, value_type), _) => is_numeric(value_type) && is_numeric(rhs_type),
        (_, Dictionary(_, value_type)) => is_numeric(lhs_type) && is_numeric(value_type),
        _ => is_numeric(lhs_type) && is_numeric(rhs_type),
    }
}

/// Coercion rules for Dictionaries: the type that both lhs and rhs
/// can be casted to for the purpose of a computation.
///
/// Not all operators support dictionaries, if `preserve_dictionaries` is true
/// dictionaries will be preserved if possible
fn dictionary_coercion(
    lhs_type: &DataType,
    rhs_type: &DataType,
    preserve_dictionaries: bool,
) -> Option<DataType> {
    use arrow::datatypes::DataType::*;
    match (lhs_type, rhs_type) {
        (
            Dictionary(_lhs_index_type, lhs_value_type),
            Dictionary(_rhs_index_type, rhs_value_type),
        ) => comparison_coercion(lhs_value_type, rhs_value_type),
        (d @ Dictionary(_, value_type), other_type)
        | (other_type, d @ Dictionary(_, value_type))
            if preserve_dictionaries && value_type.as_ref() == other_type =>
        {
            Some(d.clone())
        }
        (Dictionary(_index_type, value_type), _) => {
            comparison_coercion(value_type, rhs_type)
        }
        (_, Dictionary(_index_type, value_type)) => {
            comparison_coercion(lhs_type, value_type)
        }
        _ => None,
    }
}

/// Coercion rules for string concat.
/// This is a union of string coercion rules and specified rules:
/// 1. At lease one side of lhs and rhs should be string type (Utf8 / LargeUtf8)
/// 2. Data type of the other side should be able to cast to string type
fn string_concat_coercion(lhs_type: &DataType, rhs_type: &DataType) -> Option<DataType> {
    use arrow::datatypes::DataType::*;
    string_coercion(lhs_type, rhs_type).or(match (lhs_type, rhs_type) {
        (Utf8, from_type) | (from_type, Utf8) => {
            string_concat_internal_coercion(from_type, &Utf8)
        }
        (LargeUtf8, from_type) | (from_type, LargeUtf8) => {
            string_concat_internal_coercion(from_type, &LargeUtf8)
        }
        // TODO: cast between array elements (#6558)
        (List(_), from_type) | (from_type, List(_)) => Some(from_type.to_owned()),
        _ => None,
    })
}

/// We represent the JSON type as a custom arrow struct, with one field containing the
/// JSON as text
pub fn json_type() -> DataType {
    let json_struct = Fields::from(vec![Field::new("json", DataType::Utf8, false)]);
    DataType::Struct(json_struct)
}

pub fn enum_type(schema_name: String, enum_name: String) -> DataType {
    let enum_struct = Fields::from(vec![
        Field::new("enum", DataType::Utf8, false),
        Field::new(schema_name, DataType::Utf8, false),
        Field::new(enum_name, DataType::Utf8, false),
    ]);
    DataType::Struct(enum_struct)
}

pub fn extract_enum_schema_and_name(t: &DataType) -> Option<(String, String)> {
    if let DataType::Struct(fields) = t {
        if fields.len() == 3 && fields[0].name() == "enum" {
            return Some((fields[1].name().to_string(), fields[2].name().to_string()));
        }
    }
    None
}

pub fn get_arrow_return_type(
    left: &DataType,
    right: &DataType,
    arrow_type: &Operator,
) -> Result<DataType> {
    if left != &json_type() {
        Err(DataFusionError::Plan(format!(
            "Cannot use arrow access operator on non-json {left}!"
        )))
    } else if !right.is_integer() && right != &DataType::Utf8 {
        Err(DataFusionError::Plan(format!(
            "Right side of of access operator must integer or text (not {right})!"
        )))
    } else {
        match arrow_type {
            Operator::ArrowAccess => Ok(json_type()),
            Operator::LongArrow => Ok(DataType::Utf8),
            _ => unreachable!(),
        }
    }
}

fn array_coercion(lhs_type: &DataType, rhs_type: &DataType) -> Option<DataType> {
    // TODO: cast between array elements (#6558)
    if lhs_type.equals_datatype(rhs_type) {
        Some(lhs_type.to_owned())
    } else {
        None
    }
}

fn string_concat_internal_coercion(
    from_type: &DataType,
    to_type: &DataType,
) -> Option<DataType> {
    if can_cast_types(from_type, to_type) {
        Some(to_type.to_owned())
    } else {
        None
    }
}

/// Coercion rules for Strings: the type that both lhs and rhs can be
/// casted to for the purpose of a string computation
fn string_coercion(lhs_type: &DataType, rhs_type: &DataType) -> Option<DataType> {
    use arrow::datatypes::DataType::*;
    match (lhs_type, rhs_type) {
        (Utf8, Utf8) => Some(Utf8),
        (LargeUtf8, Utf8) => Some(LargeUtf8),
        (Utf8, LargeUtf8) => Some(LargeUtf8),
        (Utf8, FixedSizeBinary(_)) | (FixedSizeBinary(_), Utf8) => Some(Utf8),
        (LargeUtf8, LargeUtf8) => Some(LargeUtf8),
        // TODO: cast between array elements (#6558)
        (List(_), List(_)) => Some(lhs_type.clone()),
        (List(_), _) => Some(lhs_type.clone()),
        (_, List(_)) => Some(rhs_type.clone()),
        (Struct(f), Utf8) | (Utf8, Struct(f)) => {
            return if f.len() == 3 && f[0].name() == "enum" {
                Some(Utf8)
            } else {
                None
            }
        }
        _ => None,
    }
}

/// Coercion rules for Binaries: the type that both lhs and rhs can be
/// casted to for the purpose of a computation
fn binary_coercion(lhs_type: &DataType, rhs_type: &DataType) -> Option<DataType> {
    use arrow::datatypes::DataType::*;
    match (lhs_type, rhs_type) {
        (Binary | Utf8, Binary) | (Binary, Utf8) => Some(Binary),
        (LargeBinary | Binary | Utf8 | LargeUtf8, LargeBinary)
        | (LargeBinary, Binary | Utf8 | LargeUtf8) => Some(LargeBinary),
        _ => None,
    }
}

/// coercion rules for like operations.
/// This is a union of string coercion rules and dictionary coercion rules
pub fn like_coercion(lhs_type: &DataType, rhs_type: &DataType) -> Option<DataType> {
    string_coercion(lhs_type, rhs_type)
        .or_else(|| dictionary_coercion(lhs_type, rhs_type, false))
        .or_else(|| null_coercion(lhs_type, rhs_type))
}

/// coercion rules for regular expression comparison operations.
/// This is a union of string coercion rules and dictionary coercion rules
pub fn regex_coercion(lhs_type: &DataType, rhs_type: &DataType) -> Option<DataType> {
    string_coercion(lhs_type, rhs_type)
        .or_else(|| dictionary_coercion(lhs_type, rhs_type, false))
}

/// Checks if the TimeUnit associated with a Time32 or Time64 type is consistent,
/// as Time32 can only be used to Second and Millisecond accuracy, while Time64
/// is exclusively used to Microsecond and Nanosecond accuracy
fn is_time_with_valid_unit(datatype: DataType) -> bool {
    matches!(
        datatype,
        DataType::Time32(TimeUnit::Second)
            | DataType::Time32(TimeUnit::Millisecond)
            | DataType::Time64(TimeUnit::Microsecond)
            | DataType::Time64(TimeUnit::Nanosecond)
    )
}

/// Coercion rules for Temporal columns: the type that both lhs and rhs can be
/// casted to for the purpose of a date computation
/// For interval arithmetic, it doesn't handle datetime type +/- interval
fn temporal_coercion(lhs_type: &DataType, rhs_type: &DataType) -> Option<DataType> {
    use arrow::datatypes::DataType::*;
    use arrow::datatypes::IntervalUnit::*;
    use arrow::datatypes::TimeUnit::*;
    match (lhs_type, rhs_type) {
        // interval +/-
        (Interval(_), Interval(_)) => Some(Interval(MonthDayNano)),
        (Date64, Date32) | (Date32, Date64) => Some(Date64),
        (Timestamp(_, None), Date32) | (Date32, Timestamp(_, None)) => {
            Some(Timestamp(Nanosecond, None))
        }
        (Timestamp(_, _tz), Date32) | (Date32, Timestamp(_, _tz)) => {
            Some(Timestamp(Nanosecond, None))
        }
        (Timestamp(_, _tz), Date64) | (Date64, Timestamp(_, _tz)) => {
            Some(Timestamp(Nanosecond, _tz.clone()))
        }
        (Timestamp(lhs_unit, lhs_tz), Timestamp(rhs_unit, rhs_tz)) => {
            let tz = match (lhs_tz, rhs_tz) {
                // can't cast across timezones
                (Some(lhs_tz), Some(rhs_tz)) => {
                    if lhs_tz != rhs_tz {
                        return None;
                    } else {
                        Some(lhs_tz.clone())
                    }
                }
                (Some(lhs_tz), None) => Some(lhs_tz.clone()),
                (None, Some(rhs_tz)) => Some(rhs_tz.clone()),
                (None, None) => None,
            };

            let unit = match (lhs_unit, rhs_unit) {
                (Second, Millisecond) => Second,
                (Second, Microsecond) => Second,
                (Second, Nanosecond) => Second,
                (Millisecond, Second) => Second,
                (Millisecond, Microsecond) => Millisecond,
                (Millisecond, Nanosecond) => Millisecond,
                (Microsecond, Second) => Second,
                (Microsecond, Millisecond) => Millisecond,
                (Microsecond, Nanosecond) => Microsecond,
                (Nanosecond, Second) => Second,
                (Nanosecond, Millisecond) => Millisecond,
                (Nanosecond, Microsecond) => Microsecond,
                (l, r) => {
                    assert_eq!(l, r);
                    l.clone()
                }
            };

            Some(Timestamp(unit, tz))
        }
        _ => None,
    }
}

/// coercion rules from NULL type. Since NULL can be casted to most of types in arrow,
/// either lhs or rhs is NULL, if NULL can be casted to type of the other side, the coercion is valid.
fn null_coercion(lhs_type: &DataType, rhs_type: &DataType) -> Option<DataType> {
    match (lhs_type, rhs_type) {
        (DataType::Null, other_type) | (other_type, DataType::Null) => {
            if can_cast_types(&DataType::Null, other_type) {
                Some(other_type.clone())
            } else {
                None
            }
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;

    use datafusion_common::assert_contains;
    use datafusion_common::Result;

    use crate::Operator;

    use super::*;

    #[test]
    fn test_coercion_error() -> Result<()> {
        let result_type =
            get_input_types(&DataType::Float32, &Operator::Plus, &DataType::Utf8);

        let e = result_type.unwrap_err();
        assert_eq!(e.strip_backtrace(), "Error during planning: Cannot coerce arithmetic expression Float32 + Utf8 to valid types");
        Ok(())
    }

    #[test]
    fn test_decimal_binary_comparison_coercion() -> Result<()> {
        let input_decimal = DataType::Decimal128(20, 3);
        let input_types = [
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::Float32,
            DataType::Float64,
            DataType::Decimal128(38, 10),
            DataType::Decimal128(20, 8),
            DataType::Null,
        ];
        let result_types = [
            DataType::Decimal128(20, 3),
            DataType::Decimal128(20, 3),
            DataType::Decimal128(20, 3),
            DataType::Decimal128(23, 3),
            DataType::Decimal128(24, 7),
            DataType::Decimal128(32, 15),
            DataType::Decimal128(38, 10),
            DataType::Decimal128(25, 8),
            DataType::Decimal128(20, 3),
        ];
        let comparison_op_types = [
            Operator::NotEq,
            Operator::Eq,
            Operator::Gt,
            Operator::GtEq,
            Operator::Lt,
            Operator::LtEq,
        ];
        for (i, input_type) in input_types.iter().enumerate() {
            let expect_type = &result_types[i];
            for op in comparison_op_types {
                let (lhs, rhs) = get_input_types(&input_decimal, &op, input_type)?;
                assert_eq!(expect_type, &lhs);
                assert_eq!(expect_type, &rhs);
            }
        }
        // negative test
        let result_type =
            get_input_types(&input_decimal, &Operator::Eq, &DataType::Boolean);
        assert!(result_type.is_err());
        Ok(())
    }

    #[test]
    fn test_decimal_mathematics_op_type() {
        assert_eq!(
            coerce_numeric_type_to_decimal(&DataType::Int8).unwrap(),
            DataType::Decimal128(3, 0)
        );
        assert_eq!(
            coerce_numeric_type_to_decimal(&DataType::Int16).unwrap(),
            DataType::Decimal128(5, 0)
        );
        assert_eq!(
            coerce_numeric_type_to_decimal(&DataType::Int32).unwrap(),
            DataType::Decimal128(10, 0)
        );
        assert_eq!(
            coerce_numeric_type_to_decimal(&DataType::Int64).unwrap(),
            DataType::Decimal128(20, 0)
        );
        assert_eq!(
            coerce_numeric_type_to_decimal(&DataType::Float32).unwrap(),
            DataType::Decimal128(14, 7)
        );
        assert_eq!(
            coerce_numeric_type_to_decimal(&DataType::Float64).unwrap(),
            DataType::Decimal128(30, 15)
        );
    }

    #[test]
    fn test_dictionary_type_coercion() {
        use DataType::*;

        let lhs_type = Dictionary(Box::new(Int8), Box::new(Int32));
        let rhs_type = Dictionary(Box::new(Int8), Box::new(Int16));
        assert_eq!(dictionary_coercion(&lhs_type, &rhs_type, true), Some(Int32));
        assert_eq!(
            dictionary_coercion(&lhs_type, &rhs_type, false),
            Some(Int32)
        );

        // Since we can coerce values of Int16 to Utf8 can support this
        let lhs_type = Dictionary(Box::new(Int8), Box::new(Utf8));
        let rhs_type = Dictionary(Box::new(Int8), Box::new(Int16));
        assert_eq!(dictionary_coercion(&lhs_type, &rhs_type, true), Some(Utf8));

        // Since we can coerce values of Utf8 to Binary can support this
        let lhs_type = Dictionary(Box::new(Int8), Box::new(Utf8));
        let rhs_type = Dictionary(Box::new(Int8), Box::new(Binary));
        assert_eq!(
            dictionary_coercion(&lhs_type, &rhs_type, true),
            Some(Binary)
        );

        let lhs_type = Dictionary(Box::new(Int8), Box::new(Utf8));
        let rhs_type = Utf8;
        assert_eq!(dictionary_coercion(&lhs_type, &rhs_type, false), Some(Utf8));
        assert_eq!(
            dictionary_coercion(&lhs_type, &rhs_type, true),
            Some(lhs_type.clone())
        );

        let lhs_type = Utf8;
        let rhs_type = Dictionary(Box::new(Int8), Box::new(Utf8));
        assert_eq!(dictionary_coercion(&lhs_type, &rhs_type, false), Some(Utf8));
        assert_eq!(
            dictionary_coercion(&lhs_type, &rhs_type, true),
            Some(rhs_type.clone())
        );
    }

    macro_rules! test_coercion_binary_rule {
        ($A_TYPE:expr, $B_TYPE:expr, $OP:expr, $C_TYPE:expr) => {{
            let (lhs, rhs) = get_input_types(&$A_TYPE, &$OP, &$B_TYPE)?;
            assert_eq!(lhs, $C_TYPE);
            assert_eq!(rhs, $C_TYPE);
        }};
    }

    #[test]
    fn test_date_timestamp_arithmetic_error() -> Result<()> {
        let (lhs, rhs) = get_input_types(
            &DataType::Timestamp(TimeUnit::Nanosecond, None),
            &Operator::Minus,
            &DataType::Timestamp(TimeUnit::Millisecond, None),
        )?;
        assert_eq!(lhs.to_string(), "Timestamp(Millisecond, None)");
        assert_eq!(rhs.to_string(), "Timestamp(Millisecond, None)");

        let err = get_input_types(&DataType::Date32, &Operator::Plus, &DataType::Date64)
            .unwrap_err()
            .to_string();

        assert_contains!(
            &err,
            "Cannot get result type for temporal operation Date64 + Date64"
        );

        Ok(())
    }

    #[test]
    fn test_type_coercion() -> Result<()> {
        // test like coercion rule
        let result = like_coercion(&DataType::Utf8, &DataType::Utf8);
        assert_eq!(result, Some(DataType::Utf8));

        test_coercion_binary_rule!(
            DataType::Utf8,
            DataType::Date32,
            Operator::Eq,
            DataType::Date32
        );
        test_coercion_binary_rule!(
            DataType::Utf8,
            DataType::Date64,
            Operator::Lt,
            DataType::Date64
        );
        test_coercion_binary_rule!(
            DataType::Utf8,
            DataType::Time32(TimeUnit::Second),
            Operator::Eq,
            DataType::Time32(TimeUnit::Second)
        );
        test_coercion_binary_rule!(
            DataType::Utf8,
            DataType::Time32(TimeUnit::Millisecond),
            Operator::Eq,
            DataType::Time32(TimeUnit::Millisecond)
        );
        test_coercion_binary_rule!(
            DataType::Utf8,
            DataType::Time64(TimeUnit::Microsecond),
            Operator::Eq,
            DataType::Time64(TimeUnit::Microsecond)
        );
        test_coercion_binary_rule!(
            DataType::Utf8,
            DataType::Time64(TimeUnit::Nanosecond),
            Operator::Eq,
            DataType::Time64(TimeUnit::Nanosecond)
        );
        test_coercion_binary_rule!(
            DataType::Utf8,
            DataType::Timestamp(TimeUnit::Second, None),
            Operator::Lt,
            DataType::Timestamp(TimeUnit::Nanosecond, None)
        );
        test_coercion_binary_rule!(
            DataType::Utf8,
            DataType::Timestamp(TimeUnit::Millisecond, None),
            Operator::Lt,
            DataType::Timestamp(TimeUnit::Nanosecond, None)
        );
        test_coercion_binary_rule!(
            DataType::Utf8,
            DataType::Timestamp(TimeUnit::Microsecond, None),
            Operator::Lt,
            DataType::Timestamp(TimeUnit::Nanosecond, None)
        );
        test_coercion_binary_rule!(
            DataType::Utf8,
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            Operator::Lt,
            DataType::Timestamp(TimeUnit::Nanosecond, None)
        );
        test_coercion_binary_rule!(
            DataType::Utf8,
            DataType::Utf8,
            Operator::RegexMatch,
            DataType::Utf8
        );
        test_coercion_binary_rule!(
            DataType::Utf8,
            DataType::Utf8,
            Operator::RegexNotMatch,
            DataType::Utf8
        );
        test_coercion_binary_rule!(
            DataType::Utf8,
            DataType::Utf8,
            Operator::RegexNotIMatch,
            DataType::Utf8
        );
        test_coercion_binary_rule!(
            DataType::Dictionary(DataType::Int32.into(), DataType::Utf8.into()),
            DataType::Utf8,
            Operator::RegexMatch,
            DataType::Utf8
        );
        test_coercion_binary_rule!(
            DataType::Dictionary(DataType::Int32.into(), DataType::Utf8.into()),
            DataType::Utf8,
            Operator::RegexIMatch,
            DataType::Utf8
        );
        test_coercion_binary_rule!(
            DataType::Dictionary(DataType::Int32.into(), DataType::Utf8.into()),
            DataType::Utf8,
            Operator::RegexNotMatch,
            DataType::Utf8
        );
        test_coercion_binary_rule!(
            DataType::Dictionary(DataType::Int32.into(), DataType::Utf8.into()),
            DataType::Utf8,
            Operator::RegexNotIMatch,
            DataType::Utf8
        );
        test_coercion_binary_rule!(
            DataType::Int16,
            DataType::Int64,
            Operator::BitwiseAnd,
            DataType::Int64
        );
        test_coercion_binary_rule!(
            DataType::UInt64,
            DataType::UInt64,
            Operator::BitwiseAnd,
            DataType::UInt64
        );
        test_coercion_binary_rule!(
            DataType::Int8,
            DataType::UInt32,
            Operator::BitwiseAnd,
            DataType::Int64
        );
        test_coercion_binary_rule!(
            DataType::UInt32,
            DataType::Int32,
            Operator::BitwiseAnd,
            DataType::Int64
        );
        test_coercion_binary_rule!(
            DataType::UInt16,
            DataType::Int16,
            Operator::BitwiseAnd,
            DataType::Int32
        );
        test_coercion_binary_rule!(
            DataType::UInt32,
            DataType::UInt32,
            Operator::BitwiseAnd,
            DataType::UInt32
        );
        test_coercion_binary_rule!(
            DataType::UInt16,
            DataType::UInt32,
            Operator::BitwiseAnd,
            DataType::UInt32
        );
        Ok(())
    }

    #[test]
    fn test_type_coercion_arithmetic() -> Result<()> {
        // integer
        test_coercion_binary_rule!(
            DataType::Int32,
            DataType::UInt32,
            Operator::Plus,
            DataType::Int32
        );
        test_coercion_binary_rule!(
            DataType::Int32,
            DataType::UInt16,
            Operator::Minus,
            DataType::Int32
        );
        test_coercion_binary_rule!(
            DataType::Int8,
            DataType::Int64,
            Operator::Multiply,
            DataType::Int64
        );
        // float
        test_coercion_binary_rule!(
            DataType::Float32,
            DataType::Int32,
            Operator::Plus,
            DataType::Float32
        );
        test_coercion_binary_rule!(
            DataType::Float32,
            DataType::Float64,
            Operator::Multiply,
            DataType::Float64
        );
        // TODO add other data type
        Ok(())
    }

    fn test_math_decimal_coercion_rule(
        lhs_type: DataType,
        rhs_type: DataType,
        expected_lhs_type: DataType,
        expected_rhs_type: DataType,
    ) {
        // The coerced types for lhs and rhs, if any of them is not decimal
        let (lhs_type, rhs_type) = math_decimal_coercion(&lhs_type, &rhs_type).unwrap();
        assert_eq!(lhs_type, expected_lhs_type);
        assert_eq!(rhs_type, expected_rhs_type);
    }

    #[test]
    fn test_coercion_arithmetic_decimal() -> Result<()> {
        test_math_decimal_coercion_rule(
            DataType::Decimal128(10, 2),
            DataType::Decimal128(10, 2),
            DataType::Decimal128(10, 2),
            DataType::Decimal128(10, 2),
        );

        test_math_decimal_coercion_rule(
            DataType::Int32,
            DataType::Decimal128(10, 2),
            DataType::Decimal128(10, 0),
            DataType::Decimal128(10, 2),
        );

        test_math_decimal_coercion_rule(
            DataType::Int32,
            DataType::Decimal128(10, 2),
            DataType::Decimal128(10, 0),
            DataType::Decimal128(10, 2),
        );

        test_math_decimal_coercion_rule(
            DataType::Int32,
            DataType::Decimal128(10, 2),
            DataType::Decimal128(10, 0),
            DataType::Decimal128(10, 2),
        );

        test_math_decimal_coercion_rule(
            DataType::Int32,
            DataType::Decimal128(10, 2),
            DataType::Decimal128(10, 0),
            DataType::Decimal128(10, 2),
        );

        test_math_decimal_coercion_rule(
            DataType::Int32,
            DataType::Decimal128(10, 2),
            DataType::Decimal128(10, 0),
            DataType::Decimal128(10, 2),
        );

        Ok(())
    }

    #[test]
    fn test_type_coercion_compare() -> Result<()> {
        // boolean
        test_coercion_binary_rule!(
            DataType::Boolean,
            DataType::Boolean,
            Operator::Eq,
            DataType::Boolean
        );
        // float
        test_coercion_binary_rule!(
            DataType::Float32,
            DataType::Int64,
            Operator::Eq,
            DataType::Float32
        );
        test_coercion_binary_rule!(
            DataType::Float32,
            DataType::Float64,
            Operator::GtEq,
            DataType::Float64
        );
        // signed integer
        test_coercion_binary_rule!(
            DataType::Int8,
            DataType::Int32,
            Operator::LtEq,
            DataType::Int32
        );
        test_coercion_binary_rule!(
            DataType::Int64,
            DataType::Int32,
            Operator::LtEq,
            DataType::Int64
        );
        // unsigned integer
        test_coercion_binary_rule!(
            DataType::UInt32,
            DataType::UInt8,
            Operator::Gt,
            DataType::UInt32
        );
        // numeric/decimal
        test_coercion_binary_rule!(
            DataType::Int64,
            DataType::Decimal128(10, 0),
            Operator::Eq,
            DataType::Decimal128(20, 0)
        );
        test_coercion_binary_rule!(
            DataType::Int64,
            DataType::Decimal128(10, 2),
            Operator::Lt,
            DataType::Decimal128(22, 2)
        );
        test_coercion_binary_rule!(
            DataType::Float64,
            DataType::Decimal128(10, 3),
            Operator::Gt,
            DataType::Decimal128(30, 15)
        );
        test_coercion_binary_rule!(
            DataType::Int64,
            DataType::Decimal128(10, 0),
            Operator::Eq,
            DataType::Decimal128(20, 0)
        );
        test_coercion_binary_rule!(
            DataType::Decimal128(14, 2),
            DataType::Decimal128(10, 3),
            Operator::GtEq,
            DataType::Decimal128(15, 3)
        );

        // Binary
        test_coercion_binary_rule!(
            DataType::Binary,
            DataType::Binary,
            Operator::Eq,
            DataType::Binary
        );
        test_coercion_binary_rule!(
            DataType::Utf8,
            DataType::Binary,
            Operator::Eq,
            DataType::Binary
        );
        test_coercion_binary_rule!(
            DataType::Binary,
            DataType::Utf8,
            Operator::Eq,
            DataType::Binary
        );

        // LargeBinary
        test_coercion_binary_rule!(
            DataType::LargeBinary,
            DataType::LargeBinary,
            Operator::Eq,
            DataType::LargeBinary
        );
        test_coercion_binary_rule!(
            DataType::Binary,
            DataType::LargeBinary,
            Operator::Eq,
            DataType::LargeBinary
        );
        test_coercion_binary_rule!(
            DataType::LargeBinary,
            DataType::Binary,
            Operator::Eq,
            DataType::LargeBinary
        );
        test_coercion_binary_rule!(
            DataType::Utf8,
            DataType::LargeBinary,
            Operator::Eq,
            DataType::LargeBinary
        );
        test_coercion_binary_rule!(
            DataType::LargeBinary,
            DataType::Utf8,
            Operator::Eq,
            DataType::LargeBinary
        );
        test_coercion_binary_rule!(
            DataType::LargeUtf8,
            DataType::LargeBinary,
            Operator::Eq,
            DataType::LargeBinary
        );
        test_coercion_binary_rule!(
            DataType::LargeBinary,
            DataType::LargeUtf8,
            Operator::Eq,
            DataType::LargeBinary
        );

        // TODO add other data type
        Ok(())
    }

    #[test]
    fn test_type_coercion_logical_op() -> Result<()> {
        test_coercion_binary_rule!(
            DataType::Boolean,
            DataType::Boolean,
            Operator::And,
            DataType::Boolean
        );

        test_coercion_binary_rule!(
            DataType::Boolean,
            DataType::Boolean,
            Operator::Or,
            DataType::Boolean
        );
        test_coercion_binary_rule!(
            DataType::Boolean,
            DataType::Null,
            Operator::And,
            DataType::Boolean
        );
        test_coercion_binary_rule!(
            DataType::Boolean,
            DataType::Null,
            Operator::Or,
            DataType::Boolean
        );
        test_coercion_binary_rule!(
            DataType::Null,
            DataType::Null,
            Operator::Or,
            DataType::Boolean
        );
        test_coercion_binary_rule!(
            DataType::Null,
            DataType::Null,
            Operator::And,
            DataType::Boolean
        );
        test_coercion_binary_rule!(
            DataType::Null,
            DataType::Boolean,
            Operator::And,
            DataType::Boolean
        );
        test_coercion_binary_rule!(
            DataType::Null,
            DataType::Boolean,
            Operator::Or,
            DataType::Boolean
        );
        Ok(())
    }

    #[test]
    fn test_json_access_result_type() -> Result<()> {
        let input_json = DataType::Struct(Fields::from(vec![Field::new(
            "json",
            DataType::Utf8,
            false,
        )]));
        let result_type =
            get_result_type(&input_json, &Operator::ArrowAccess, &DataType::Int32)?;
        assert_eq!(result_type, json_type());

        let result_type =
            get_result_type(&input_json, &Operator::LongArrow, &DataType::Int32)?;
        assert_eq!(result_type, DataType::Utf8);
        Ok(())
    }
}
