from typing import Optional, List, Dict, Tuple, Any

from tai_dynamic_postgres_mcp.gen.order.models import OrderByItem


def build_order_by_clause(
        order_by: Optional[List[OrderByItem]] = None,
        column_map: Optional[Dict[str, str]] = None
) -> Tuple[str, List[Any]]:
    parts = []
    order_params: List[Any] = []

    if not order_by:
        return "", []

    op_map = {
        'l2': '<->',
        'inner_product': '<#>',
        'cosine': '<=>'
    }

    for item in order_by:
        mapped_field = column_map.get(item.field, item.field) if column_map else item.field
        if item.knn:
            knn = item.knn
            op = op_map.get(knn.distance, '<->')
            vector_str = '[' + ','.join(map(str, knn.query)) + ']'
            parts.append(f"{mapped_field} {op} (%s)::vector {knn.direction}")
            order_params.append(vector_str)
        else:
            parts.append(f"{mapped_field} {item.direction}")

    return " ORDER BY " + ", ".join(parts), order_params
