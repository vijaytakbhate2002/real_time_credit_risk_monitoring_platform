"""
schema.py
─────────
Feature schema for the CreditFlow validation layer.

Each feature selected in feature_selection.ipynb is declared here as a
``FieldRule``.  The ``BatchValidator`` reads this schema to:

  1. Build the PySpark ``StructType`` used by ``from_json`` to parse Kafka payloads.
  2. Derive all validation checks (null, dtype, allowed values, numeric range).

Adding or changing a feature only requires editing this file.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, FrozenSet, Optional

from pyspark.sql.types import (
    DataType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


# ── Field Rule ───────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class FieldRule:
    """
    Validation contract for a single feature field.

    Parameters
    ----------
    spark_type:
        PySpark DataType used for JSON parsing and dtype validation.
    nullable:
        If False the field must be present and non-null in every record.
    default:
        When ``nullable=True`` and the field is null, fill it with this value.
        ``None`` means: leave the null as-is (nullable and no default).
    allowed_values:
        For categorical/enum fields — set of valid string values.
        Records with any other value are rejected.
    min_value:
        Inclusive lower bound for numeric fields.
    max_value:
        Inclusive upper bound for numeric fields.
    description:
        Human-readable note about the field (shown in rejection messages).
    """

    spark_type: DataType
    nullable: bool = True
    default: Optional[Any] = None
    allowed_values: Optional[FrozenSet[str]] = None
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    description: str = ""


# ── Feature Schema ───────────────────────────────────────────────────────────
# Every key must match the JSON field name produced by the simulator.
# 43 features total: 36 original + 7 derived.

FEATURE_SCHEMA: dict[str, FieldRule] = {

    # ── Loan / Credit Amounts ─────────────────────────────────────────────
    "AMT_CREDIT": FieldRule(
        spark_type=DoubleType(),
        nullable=False,
        min_value=0.0,
        description="Total credit amount applied for.",
    ),
    "AMT_ANNUITY": FieldRule(
        spark_type=DoubleType(),
        nullable=False,
        min_value=0.0,
        description="EMI / annuity amount.",
    ),
    "AMT_GOODS_PRICE": FieldRule(
        spark_type=DoubleType(),
        nullable=True,
        min_value=0.0,
        description="Price of goods for consumer credit; null for cash loans.",
    ),
    "AMT_INCOME_TOTAL": FieldRule(
        spark_type=DoubleType(),
        nullable=False,
        min_value=0.0,
        description="Total annual income.",
    ),

    # ── DPD / Delinquency Signals ─────────────────────────────────────────
    "OBS_30_CNT_SOCIAL_CIRCLE": FieldRule(
        spark_type=DoubleType(),
        nullable=True,
        default=0.0,
        min_value=0.0,
        description="Observations with DPD30+ in social circle.",
    ),
    "DEF_30_CNT_SOCIAL_CIRCLE": FieldRule(
        spark_type=DoubleType(),
        nullable=True,
        default=0.0,
        min_value=0.0,
        description="Defaults with DPD30+ in social circle.",
    ),
    "OBS_60_CNT_SOCIAL_CIRCLE": FieldRule(
        spark_type=DoubleType(),
        nullable=True,
        default=0.0,
        min_value=0.0,
        description="Observations with DPD60+ in social circle.",
    ),
    "DEF_60_CNT_SOCIAL_CIRCLE": FieldRule(
        spark_type=DoubleType(),
        nullable=True,
        default=0.0,
        min_value=0.0,
        description="Defaults with DPD60+ in social circle (hard-cut signal).",
    ),

    # ── Credit Bureau Enquiry Counts (Hard-Cut Signals) ───────────────────
    "AMT_REQ_CREDIT_BUREAU_HOUR": FieldRule(
        spark_type=DoubleType(),
        nullable=True,
        default=0.0,
        min_value=0.0,
        description="Credit enquiries in the last hour.",
    ),
    "AMT_REQ_CREDIT_BUREAU_DAY": FieldRule(
        spark_type=DoubleType(),
        nullable=True,
        default=0.0,
        min_value=0.0,
        description="Credit enquiries in the last day.",
    ),
    "AMT_REQ_CREDIT_BUREAU_WEEK": FieldRule(
        spark_type=DoubleType(),
        nullable=True,
        default=0.0,
        min_value=0.0,
        description="Credit enquiries in the last week.",
    ),
    "AMT_REQ_CREDIT_BUREAU_MON": FieldRule(
        spark_type=DoubleType(),
        nullable=True,
        default=0.0,
        min_value=0.0,
        description="Credit enquiries in the last month.",
    ),
    "AMT_REQ_CREDIT_BUREAU_QRT": FieldRule(
        spark_type=DoubleType(),
        nullable=True,
        default=0.0,
        min_value=0.0,
        description="Credit enquiries in the last quarter.",
    ),
    "AMT_REQ_CREDIT_BUREAU_YEAR": FieldRule(
        spark_type=DoubleType(),
        nullable=True,
        default=0.0,
        min_value=0.0,
        description="Credit enquiries in the last year.",
    ),

    # ── External Risk Scores ──────────────────────────────────────────────
    "EXT_SOURCE_1": FieldRule(
        spark_type=DoubleType(),
        nullable=True,
        min_value=0.0,
        max_value=1.0,
        description="External bureau score 1 (0–1 range).",
    ),
    "EXT_SOURCE_2": FieldRule(
        spark_type=DoubleType(),
        nullable=True,
        min_value=0.0,
        max_value=1.0,
        description="External bureau score 2 (0–1 range).",
    ),
    "EXT_SOURCE_3": FieldRule(
        spark_type=DoubleType(),
        nullable=True,
        min_value=0.0,
        max_value=1.0,
        description="External bureau score 3 (0–1 range).",
    ),

    # ── Days-Based Features ───────────────────────────────────────────────
    "DAYS_BIRTH": FieldRule(
        spark_type=IntegerType(),
        nullable=False,
        max_value=-1,
        description="Age in days relative to application (always negative).",
    ),
    "DAYS_EMPLOYED": FieldRule(
        spark_type=IntegerType(),
        nullable=False,
        description="Days employed relative to application (negative = employed; 365243 = unemployed/pensioner).",
    ),
    "DAYS_REGISTRATION": FieldRule(
        spark_type=DoubleType(),
        nullable=False,
        description="Days since registration document was last changed.",
    ),
    "DAYS_ID_PUBLISH": FieldRule(
        spark_type=IntegerType(),
        nullable=False,
        description="Days since identity document was last changed.",
    ),
    "DAYS_LAST_PHONE_CHANGE": FieldRule(
        spark_type=DoubleType(),
        nullable=True,
        default=0.0,
        description="Days since last phone number change.",
    ),

    # ── Demographics & Application Metadata ───────────────────────────────
    "NAME_CONTRACT_TYPE": FieldRule(
        spark_type=StringType(),
        nullable=False,
        allowed_values=frozenset({"Cash loans", "Revolving loans"}),
        description="Loan contract type.",
    ),
    "CODE_GENDER": FieldRule(
        spark_type=StringType(),
        nullable=False,
        allowed_values=frozenset({"M", "F", "XNA"}),
        description="Applicant gender code.",
    ),
    "FLAG_OWN_CAR": FieldRule(
        spark_type=StringType(),
        nullable=False,
        allowed_values=frozenset({"Y", "N"}),
        description="Whether the applicant owns a car.",
    ),
    "FLAG_OWN_REALTY": FieldRule(
        spark_type=StringType(),
        nullable=False,
        allowed_values=frozenset({"Y", "N"}),
        description="Whether the applicant owns real estate.",
    ),
    "CNT_CHILDREN": FieldRule(
        spark_type=IntegerType(),
        nullable=False,
        min_value=0.0,
        description="Number of children.",
    ),
    "CNT_FAM_MEMBERS": FieldRule(
        spark_type=DoubleType(),
        nullable=True,
        default=1.0,
        min_value=1.0,
        description="Total family size (including applicant).",
    ),
    "NAME_INCOME_TYPE": FieldRule(
        spark_type=StringType(),
        nullable=False,
        description="Applicant income / employment category.",
    ),
    "NAME_EDUCATION_TYPE": FieldRule(
        spark_type=StringType(),
        nullable=False,
        description="Highest education level attained.",
    ),
    "NAME_FAMILY_STATUS": FieldRule(
        spark_type=StringType(),
        nullable=False,
        description="Marital / family status.",
    ),
    "NAME_HOUSING_TYPE": FieldRule(
        spark_type=StringType(),
        nullable=False,
        description="Type of housing.",
    ),
    "OCCUPATION_TYPE": FieldRule(
        spark_type=StringType(),
        nullable=True,
        description="Applicant occupation (nullable — not always disclosed).",
    ),
    "ORGANIZATION_TYPE": FieldRule(
        spark_type=StringType(),
        nullable=True,
        description="Employer organisation type (nullable).",
    ),
    "REGION_RATING_CLIENT": FieldRule(
        spark_type=IntegerType(),
        nullable=False,
        allowed_values=frozenset({"1", "2", "3"}),
        description="Region risk rating (1 = low, 3 = high).",
    ),
    "REGION_RATING_CLIENT_W_CITY": FieldRule(
        spark_type=IntegerType(),
        nullable=False,
        allowed_values=frozenset({"1", "2", "3"}),
        description="Region risk rating including city adjustment.",
    ),

    # ── Derived Features ──────────────────────────────────────────────────
    "DEBT_TO_INCOME": FieldRule(
        spark_type=DoubleType(),
        nullable=True,
        min_value=0.0,
        description="AMT_CREDIT / AMT_INCOME_TOTAL.",
    ),
    "ANNUITY_TO_INCOME": FieldRule(
        spark_type=DoubleType(),
        nullable=True,
        min_value=0.0,
        description="AMT_ANNUITY / AMT_INCOME_TOTAL (EMI burden ratio).",
    ),
    "LOAN_TO_VALUE": FieldRule(
        spark_type=DoubleType(),
        nullable=True,
        min_value=0.0,
        description="AMT_CREDIT / AMT_GOODS_PRICE.",
    ),
    "AGE_YEARS": FieldRule(
        spark_type=DoubleType(),
        nullable=False,
        min_value=18.0,
        max_value=100.0,
        description="Applicant age in years (derived from DAYS_BIRTH).",
    ),
    "YEARS_EMPLOYED": FieldRule(
        spark_type=DoubleType(),
        nullable=True,
        min_value=0.0,
        description="Years employed (null when DAYS_EMPLOYED is anomalous).",
    ),
    "TOTAL_DPD_SOCIAL_CIRCLE": FieldRule(
        spark_type=DoubleType(),
        nullable=False,
        min_value=0.0,
        description="Sum of DEF_30 + DEF_60 social circle defaults.",
    ),
    "TOTAL_BUREAU_ENQUIRIES": FieldRule(
        spark_type=DoubleType(),
        nullable=False,
        min_value=0.0,
        description="Sum of all credit bureau enquiry windows.",
    ),
}


# ── Helpers ──────────────────────────────────────────────────────────────────

def build_spark_schema() -> StructType:
    """
    Build a PySpark ``StructType`` from ``FEATURE_SCHEMA``.

    Used by ``from_json`` to parse Kafka message payloads.
    The top-level ``application_id`` field is prepended automatically.
    """
    fields = [StructField("application_id", StringType(), nullable=False)]
    for name, rule in FEATURE_SCHEMA.items():
        # Always mark nullable=True at parse time; strict null enforcement
        # happens in BatchValidator after parsing so we capture the error.
        fields.append(StructField(name, rule.spark_type, nullable=True))
    return StructType(fields)
