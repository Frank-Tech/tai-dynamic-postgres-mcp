from typing import Optional, List, Any, Dict, Tuple

from tai_dynamic_postgres_mcp.gen.filters.models import WhereFilter, LogicalFilter


def build_where_clause(
        op: Optional[WhereFilter], column_map: Optional[Dict[str, str]] = None
) -> Tuple[str, List[Any]]:
    if not op:
        return "", []

    inner = op.root
    clauses = []
    params = []

    def map_column(field: str) -> str:
        if column_map and isinstance(column_map, dict):
            return column_map.get(field, field)
        return field

    if isinstance(inner, LogicalFilter):
        if inner.AND:
            sub_clauses = [build_where_clause(sub, column_map) for sub in inner.AND]
            sql_parts, values = zip(*sub_clauses)
            clauses.append(f"({' AND '.join([part for part in sql_parts if part])})")
            params.extend([v for subvals in values for v in subvals])
        if inner.OR:
            sub_clauses = [build_where_clause(sub, column_map) for sub in inner.OR]
            sql_parts, values = zip(*sub_clauses)
            clauses.append(f"({' OR '.join([part for part in sql_parts if part])})")
            params.extend([v for subvals in values for v in subvals])
        if inner.NOT:
            sql, vals = build_where_clause(inner.NOT, column_map)
            if sql:
                clauses.append(f"(NOT ({sql}))")
            params.extend(vals)
    elif isinstance(inner, dict):
        for field, condition in inner.items():
            mapped_field = map_column(field)
            for operator, value in condition.model_dump(exclude_none=True, by_alias=True).items():
                if operator == "knn":
                    knn = condition.knn
                    op_map = {
                        'l2': '<->',
                        'inner_product': '<#>',
                        'cosine': '<=>'
                    }
                    op = op_map.get(knn.distance, '<->')
                    vector_str = '[' + ','.join(map(str, knn.query)) + ']'
                    clauses.append(f"{mapped_field} {op} (%s)::vector < %s")
                    params.extend([vector_str, knn.threshold])
                else:
                    match operator:
                        case "eq":
                            clauses.append(f"{mapped_field} = %s")
                            params.append(value)
                        case "ne":
                            clauses.append(f"{mapped_field} != %s")
                            params.append(value)
                        case "gt":
                            clauses.append(f"{mapped_field} > %s")
                            params.append(value)
                        case "gte":
                            clauses.append(f"{mapped_field} >= %s")
                            params.append(value)
                        case "lt":
                            clauses.append(f"{mapped_field} < %s")
                            params.append(value)
                        case "lte":
                            clauses.append(f"{mapped_field} <= %s")
                            params.append(value)
                        case "like":
                            clauses.append(f"{mapped_field} LIKE %s")
                            params.append(value)
                        case "not_like":
                            clauses.append(f"{mapped_field} NOT LIKE %s")
                            params.append(value)
                        case "ilike":
                            clauses.append(f"{mapped_field} ILIKE %s")
                            params.append(value)
                        case "not_ilike":
                            clauses.append(f"{mapped_field} NOT ILIKE %s")
                            params.append(value)
                        case "is_null":
                            clauses.append(f"{mapped_field} IS {'NULL' if value else 'NOT NULL'}")
                        case "in":
                            placeholders = ', '.join(['%s'] * len(value))
                            clauses.append(f"{mapped_field} IN ({placeholders})")
                            params.extend(value)
                        case "not_in":
                            placeholders = ', '.join(['%s'] * len(value))
                            clauses.append(f"{mapped_field} NOT IN ({placeholders})")
                            params.extend(value)
                        case "between":
                            clauses.append(f"{mapped_field} BETWEEN %s AND %s")
                            params.extend(value)

    where_clause = " AND ".join([clause for clause in clauses if clause]) if clauses else ""
    return where_clause, params
