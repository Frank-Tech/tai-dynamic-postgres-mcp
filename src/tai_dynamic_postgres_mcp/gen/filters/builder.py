from typing import Optional, List, Any

from tai_dynamic_postgres_mcp.gen.filters.models import WhereFilter, LogicalFilter


def build_where_clause(op: Optional[WhereFilter]) -> tuple[str, List[Any]]:
    if not op:
        return "", []

    inner = op.root
    clauses = []
    params = []

    if isinstance(inner, LogicalFilter):
        if inner.AND:
            sub_clauses = [build_where_clause(sub) for sub in inner.AND]
            sql_parts, values = zip(*sub_clauses)
            clauses.append(f"({' AND '.join([part for part in sql_parts if part])})")
            params.extend([v for subvals in values for v in subvals])
        if inner.OR:
            sub_clauses = [build_where_clause(sub) for sub in inner.OR]
            sql_parts, values = zip(*sub_clauses)
            clauses.append(f"({' OR '.join([part for part in sql_parts if part])})")
            params.extend([v for subvals in values for v in subvals])
        if inner.NOT:
            sql, vals = build_where_clause(inner.NOT)
            if sql:
                clauses.append(f"(NOT ({sql}))")
            params.extend(vals)
    elif isinstance(inner, dict):
        for field, condition in inner.items():
            for operator, value in condition.model_dump(exclude_none=True, by_alias=True).items():
                match operator:
                    case "eq":
                        clauses.append(f"{field} = %s")
                        params.append(value)
                    case "ne":
                        clauses.append(f"{field} != %s")
                        params.append(value)
                    case "gt":
                        clauses.append(f"{field} > %s")
                        params.append(value)
                    case "gte":
                        clauses.append(f"{field} >= %s")
                        params.append(value)
                    case "lt":
                        clauses.append(f"{field} < %s")
                        params.append(value)
                    case "lte":
                        clauses.append(f"{field} <= %s")
                        params.append(value)
                    case "like":
                        clauses.append(f"{field} LIKE %s")
                        params.append(value)
                    case "not_like":
                        clauses.append(f"{field} NOT LIKE %s")
                        params.append(value)
                    case "ilike":
                        clauses.append(f"{field} ILIKE %s")
                        params.append(value)
                    case "not_ilike":
                        clauses.append(f"{field} NOT ILIKE %s")
                        params.append(value)
                    case "is_null":
                        clauses.append(f"{field} IS {'NULL' if value else 'NOT NULL'}")
                    case "in_":
                        placeholders = ', '.join(['%s'] * len(value))
                        clauses.append(f"{field} IN ({placeholders})")
                        params.extend(value)
                    case "not_in":
                        placeholders = ', '.join(['%s'] * len(value))
                        clauses.append(f"{field} NOT IN ({placeholders})")
                        params.extend(value)
                    case "between":
                        clauses.append(f"{field} BETWEEN %s AND %s")
                        params.extend(value)

    where_clause = " AND ".join([clause for clause in clauses if clause]) if clauses else ""
    return where_clause, params
