from typing import Literal, List, Optional

from pydantic import BaseModel


class KnnOrder(BaseModel):
    query: List[float]
    distance: Literal['l2', 'inner_product', 'cosine'] = 'l2'
    direction: Literal['ASC', 'DESC'] = 'ASC'


class OrderByItem(BaseModel):
    field: str
    direction: Literal['ASC', 'DESC'] = 'ASC'
    knn: Optional[KnnOrder] = None
