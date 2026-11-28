"""
CreditFlow — Single-Request JSON Validator

Validates one input JSON dict (a single loan application payload coming
directly from the backend) against the feature schema declared in schema.py.

Usage
-----
    from src.validation import RequestValidator

    validator = RequestValidator()
    result = validator.validate(payload)

    if result.is_valid:
        process(result.data)        # clean, defaulted dict
    else:
        reject(result.errors)       # list of human-readable error strings
"""

from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any
from pyspark.sql.types import DoubleType, IntegerType, StringType
from src.schema import FEATURE_SCHEMA, FieldRule


_SPARK_TO_PYTHON: dict[type, type] = {
    DoubleType:  float,
    IntegerType: int,
    StringType:  str,
}


@dataclass
class ValidationResult:
    """
    Outcome of validating a single request payload.

    Attributes
    ----------
    is_valid:
        True when every field passed all schema checks.
    data:
        Cleaned and defaulted payload dict.  Populated only when ``is_valid``
        is True; empty dict otherwise.
    errors:
        Human-readable violation messages.  Empty list when ``is_valid`` is True.
    """

    is_valid: bool
    data:     dict[str, Any]  = field(default_factory=dict)
    errors:   list[str]       = field(default_factory=list)



class RequestValidator:
    """
    Validates a single input JSON dict against the CreditFlow feature schema.

    Checks (in order per field)
    ---------------------------
    1. Apply default  — if the field is null/missing and a default is declared,
                        fill it before running any other check.
    2. Required null  — non-nullable fields must be present and non-null.
    3. Type coercion  — the value must be castable to the field's declared type.
    4. Allowed values — categorical fields must match one of the allowed values.
    5. Min / max      — numeric fields must be within the declared bounds.

    Parameters
    ----------
    schema:
        ``{field_name: FieldRule}`` dict.  Defaults to the project-wide
        ``FEATURE_SCHEMA`` from schema.py.
    """

    def __init__(self, schema: dict[str, FieldRule] = FEATURE_SCHEMA) -> None:
        self._schema = schema


    def validate(self, payload: dict[str, Any]) -> ValidationResult:
        """
        Validate a single input JSON payload.

        Parameters
        ----------
        payload:
            Raw dict parsed from the backend request body.

        Returns
        -------
        ValidationResult
            ``.is_valid`` / ``.data`` (clean dict) / ``.errors`` (list of strings).
        """
        data   = dict(payload)   
        errors: list[str] = []

        for field_name, rule in self._schema.items():
            value = data.get(field_name)

            if value is None and rule.nullable and rule.default is not None:
                data[field_name] = rule.default
                value = rule.default

            if value is None and not rule.nullable:
                errors.append(f"[{field_name}] required field is missing or null")
                continue   

            if value is not None:
                value, type_error = self._coerce(field_name, value, rule)
                if type_error:
                    errors.append(type_error)
                    continue   
                data[field_name] = value

            if value is not None and rule.allowed_values:
                if str(value) not in rule.allowed_values:
                    errors.append(
                        f"[{field_name}] '{value}' not in allowed values "
                        f"{sorted(rule.allowed_values)}"
                    )

            if value is not None and rule.min_value is not None:
                if float(value) < rule.min_value:
                    errors.append(
                        f"[{field_name}] {value} < min({rule.min_value})"
                    )

            if value is not None and rule.max_value is not None:
                if float(value) > rule.max_value:
                    errors.append(
                        f"[{field_name}] {value} > max({rule.max_value})"
                    )

        if errors:
            return ValidationResult(is_valid=False, errors=errors)

        return ValidationResult(is_valid=True, data=data)


    def _coerce(
        self,
        field_name: str,
        value: Any,
        rule: FieldRule,
    ) -> tuple[Any, str | None]:
        """
        Attempt to cast ``value`` to the Python type mapped from ``rule.spark_type``.

        Returns
        -------
        (coerced_value, None)   on success
        (None, error_message)   on failure
        """
        python_type = _SPARK_TO_PYTHON.get(type(rule.spark_type))

        if python_type is None:
            return value, None

        try:
            return python_type(value), None
        except (ValueError, TypeError):
            return None, (
                f"[{field_name}] cannot cast '{value}' "
                f"to {python_type.__name__}"
            )
