from copy import deepcopy
from functools import partial
from typing import Callable, Optional

from bw2io.utils import rescale_exchange
from loguru import logger

from .node_classes import Process


def generic_allocation(
    act: Process,
    func: Callable,
    strategy_label: Optional[str] = None,
) -> dict:
    """Allocation by single allocation factor generated by `func`.

    Allocation amount is edge amount times function(edge_data, act) divided by sum of all edge
    amounts times function(edge_data, act).

    Skips functional edges with zero allocation values."""
    if not isinstance(act, Process):
        raise TypeError

    total = 0
    for product in act.products():
        total += func(product._data, act)

    allocated_products = {}
    for product in act.products():
        allocated_product = deepcopy(product._data)
        allocated_product["exchanges"] = []

        factor = func(product._data, act) / total
        if not factor:
            continue

        logger.debug(
            "Using allocation factor {f} for product {e} on activity {a}",
            f=factor,
            e=repr(product),
            a=repr(act),
        )

        for exchange in act.exchanges():
            if exchange["type"] == "production":
                continue

            new_exchange = rescale_exchange(deepcopy(exchange._data), factor)
            new_exchange["output"] = product.key

            allocated_product["exchanges"].append(new_exchange)

        allocated_products[product.key] = allocated_product

    return allocated_products


def get_allocation_factor_from_property(
    product_data: dict, node: Process, property_label: str
) -> float:
    if "properties" not in product_data:
        raise KeyError(
            f"Product {product_data["name"]} from process {node} (id {node.id}) doesn't have properties"
        )
    try:
        return product_data["amount"] * product_data["properties"][property_label]
    except KeyError as err:
        raise KeyError(
            f"Edge {product_data} from process {node} (id {node.id}) missing property {property_label}"
        ) from err


def property_allocation(property_label: str) -> Callable:
    return partial(
        generic_allocation,
        func=partial(
            get_allocation_factor_from_property, property_label=property_label
        ),
        strategy_label=f"property allocation by '{property_label}'"
    )


allocation_strategies = {
    "price": property_allocation("price"),
    "manual": property_allocation("manual"),
    "mass": property_allocation("mass"),
    "equal": partial(generic_allocation, func=lambda x, y: 1.0),
}
