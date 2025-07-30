from __future__ import annotations

from typing import Optional, List, Dict, Any

from pydantic import BaseModel, RootModel
from pydantic import Field


class FilterOp(BaseModel):
    eq: Optional[Any] = None  # Equal
    ne: Optional[Any] = None  # Not equal
    gt: Optional[Any] = None  # Greater than
    gte: Optional[Any] = None  # Greater than or equal
    lt: Optional[Any] = None  # Less than
    lte: Optional[Any] = None  # Less than or equal

    like: Optional[str] = None  # LIKE '%value%'
    not_like: Optional[str] = None  # NOT LIKE
    ilike: Optional[str] = None  # ILIKE (PostgreSQL only)
    not_ilike: Optional[str] = None  # NOT ILIKE (PostgreSQL only)

    in_: Optional[List[Any]] = Field(default=None, alias="in")  # IN (...)
    not_in: Optional[List[Any]] = None  # NOT IN (...)

    between: Optional[List[Any]] = None  # BETWEEN [x, y]
    is_null: Optional[bool] = None  # IS NULL / IS NOT NULL


class BaseWhereFilter(RootModel[Dict[str, FilterOp]]):
    pass


class WhereFilter(BaseModel):
    AND: Optional[List[WhereFilter]] = None
    OR: Optional[List[WhereFilter]] = None
    NOT: Optional[WhereFilter] = None
    base: Optional[BaseWhereFilter] = None
