from copy import deepcopy
from functools import partial
from typing import Callable, List, Optional, Union
from uuid import uuid4

from bw2data import get_node
from bw2data.backends.proxies import Activity
from bw2data.errors import UnknownObject
from bw2io.utils import rescale_exchange
from loguru import logger

from .supplemental import add_product_node_properties_to_exchange
from .node_classes import Process, Function


def remove_output(d: dict) -> dict:
    if "output" in d:
        del d["output"]
    return d


def generic_allocation(
    process: Process,
    getter: lambda function: float(),
) -> List[dict]:
    """Allocation by single allocation factor generated by `func`.

    Allocation amount is edge amount times function(edge_data, act) divided by sum of all edge
    amounts times function(edge_data, act).

    **No longer** skips functional edges with zero allocation values."""
    if not isinstance(process, Process):
        raise ValueError("Activity must be a Process instance")

    if not process.multifunctional:
        return []

    total = sum([getter(function) for function in process.functions()])

    if not total:
        raise ZeroDivisionError("Sum of allocation factors is zero")

    allocated_processes = []

    for i, function in enumerate(process.functions()):
        factor = getter(function) / total
        function["allocation_factor"] = factor
        function.save()

        logger.debug(f"Using allocation factor {factor} for function {function} on process {process}")

        allocated_ds = deepcopy(dict(process))

        del allocated_ds["id"]

        allocated_ds["name"] = f"{process["name"]} ~ {function["name"]}"
        allocated_ds["code"] = function["code"] + "-allocated"
        allocated_ds["full_process_key"] = process.key
        allocated_ds["type"] = "readonly_process"
        allocated_ds["exchanges"] = []

        # iterate over all exchanges in the process
        for exc_ds in [dict(exc) for exc in process.exchanges()]:
            # skip if it's a functional exchange other than the one we're allocating now
            if exc_ds["type"] in ["production", "reduction"] and exc_ds["input"] != function.key:
                continue
            # allocate if it's not a functional exchange
            elif exc_ds["type"] not in ["production", "reduction"]:
                exc_ds["amount"] = exc_ds["amount"] * factor
            # write to dataset
            allocated_ds["exchanges"].append(exc_ds)

        allocated_processes.append(allocated_ds)

    return allocated_processes


def get_allocation_factor_from_property(
    function: Function,
    property_label: str,
    normalize_by_amount: bool = True,
) -> float:
    if not function.get("properties"):
        raise KeyError(f"Function {function} from process {function.processor} doesn't have properties")

    try:
        if normalize_by_amount:
            return function.processing_edge["amount"] * function["properties"][property_label]
        else:
            return function["properties"][property_label]
    except KeyError as e:
        raise KeyError(f"Function {function} from {function.processor} missing property {property_label}") from e


def property_allocation(property_label: str, normalize_by_amount: bool = True) -> Callable:
    getter = partial(
        get_allocation_factor_from_property,
        property_label=property_label,
        normalize_by_amount=normalize_by_amount
    )

    return partial(generic_allocation, getter=getter)


allocation_strategies = {
    "price": property_allocation("price"),
    "manual_allocation": property_allocation(
        "manual_allocation", normalize_by_amount=False
    ),
    "mass": property_allocation("mass"),
    "equal": partial(generic_allocation, getter=lambda x: 1.0),
}
