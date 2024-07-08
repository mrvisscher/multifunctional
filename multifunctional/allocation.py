from copy import deepcopy
from functools import partial
from typing import Callable, List, Optional, Union
from uuid import uuid4

from bw2data import get_node
from bw2data.backends.proxies import Activity
from bw2data.errors import UnknownObject
from bw2io.utils import rescale_exchange
from loguru import logger


def remove_output(d: dict) -> dict:
    if "output" in d:
        del d["output"]
    return d


def generic_allocation(
    act: Union[dict, Activity],
    func: Callable,
    strategy_label: Optional[str] = None,
) -> List[dict]:
    """Allocation by single allocation factor generated by `func`.

    Allocation amount is edge amount times function(edge_data, act) divided by sum of all edge
    amounts times function(edge_data, act).

    Skips functional edges with zero allocation values."""
    if isinstance(act, Activity):
        act_data = act._data
        act_data["exchanges"] = [exc._data for exc in act.exchanges()]
        act = act_data

    if act.get("type") == "readonly_process":
        return []
    elif sum(1 for exc in act.get("exchanges", []) if exc.get("functional")) < 2:
        return []

    total = 0
    for exc in filter(lambda x: x.get("functional"), act.get("exchanges", [])):
        total += func(exc, act)

    if not total:
        raise ZeroDivisionError("Sum of allocation factors is zero")

    processes = [act]

    for original_exc in filter(lambda x: x.get("functional"), act.get("exchanges", [])):
        new_exc = remove_output(deepcopy(original_exc))

        factor = func(original_exc, act) / total
        if not factor:
            continue

        logger.debug(
            "Using allocation factor {f} for functional edge {e} on activity {a}",
            f=factor,
            e=repr(original_exc),
            a=repr(act),
        )

        # Added by `add_exchange_input_if_missing`, but shouldn't be used
        # by read-only processes. This code is only ever triggered once
        if new_exc.get("mf_artificial_code"):
            del original_exc["mf_artificial_code"]
            del new_exc["mf_artificial_code"]
            del new_exc["input"]

        if original_exc.get("mf_allocated"):
            # Don't need to think, made choice on initial allocation
            process_code = original_exc["mf_allocated_process_code"]
        elif new_exc.get("input"):
            # Initial allocation
            original_exc["mf_allocated"] = True
            # We have a link to a known (product) node, but need to generate the code
            # for the separate read-only process
            original_exc["mf_manual_input_product"] = True
            process_code = original_exc["mf_allocated_process_code"] = uuid4().hex
        else:
            # Initial allocation
            original_exc["mf_allocated"] = True
            # Create new process+product node with same generated code
            # This code can come from `desired_code` or be random
            process_code = original_exc["mf_allocated_process_code"] = (
                original_exc.get("desired_code") or uuid4().hex
            )
            original_exc["mf_manual_input_product"] = False
            original_exc["input"] = new_exc["input"] = (act["database"], process_code)
            logger.debug(
                "Creating new product code {c} for functional edge:\n{e}\nOn activity\n{a}",
                c=process_code,
                e=repr(original_exc),
                a=repr(act),
            )

        if original_exc["mf_manual_input_product"]:
            # Get product name and unit attributes from the separate node, if available
            try:
                product = get_node(database=new_exc["input"][0], code=new_exc["input"][1])
            except UnknownObject:
                # Try using attributes stored on the edge
                # Might not work, but better than trying to give access to whole raw database
                # currently being written
                product = new_exc
        else:
            product = None

        allocated_process = deepcopy(act)
        if "id" in allocated_process:
            del allocated_process["id"]
        if strategy_label:
            allocated_process["mf_strategy_label"] = strategy_label
        allocated_process["code"] = process_code
        allocated_process["mf_parent_key"] = (act["database"], act["code"])
        allocated_process["type"] = "readonly_process"
        allocated_process["production amount"] = original_exc["amount"]
        if product:
            allocated_process["reference product"] = product.get("name", "(unknown)")
            allocated_process["unit"] = product.get("unit", "(unknown)")
        else:
            allocated_process["reference product"] = new_exc.get("name", "(unknown)")
            allocated_process["unit"] = new_exc.get("unit") or act.get("unit", "(unknown)")
        allocated_process["exchanges"] = [new_exc]

        for other in filter(lambda x: not x.get("functional"), act["exchanges"]):
            allocated_process["exchanges"].append(
                remove_output(rescale_exchange(deepcopy(other), factor))
            )

        processes.append(allocated_process)

    return processes


def get_allocation_factor_from_property(
    edge_data: dict, node: dict, property_label: str, normalize_by_production_amount: bool = True
) -> float:
    if "properties" not in edge_data:
        raise KeyError(
            f"Edge {edge_data} from process {node.get('name')} (id {node.get('id')}) doesn't have properties"
        )
    try:
        if normalize_by_production_amount:
            return edge_data["amount"] * edge_data["properties"][property_label]
        else:
            return edge_data["properties"][property_label]
    except KeyError as err:
        raise KeyError(
            f"Edge {edge_data} from process {node.get('name')} (id {node.get('id')}) missing property {property_label}"
        ) from err


def property_allocation(
    property_label: str, normalize_by_production_amount: bool = True
) -> Callable:
    return partial(
        generic_allocation,
        func=partial(
            get_allocation_factor_from_property,
            property_label=property_label,
            normalize_by_production_amount=normalize_by_production_amount,
        ),
        strategy_label=f"property allocation by '{property_label}'",
    )


allocation_strategies = {
    "price": property_allocation("price"),
    "manual_allocation": property_allocation("manual_allocation", normalize_by_production_amount=False),
    "mass": property_allocation("mass"),
    "equal": partial(generic_allocation, func=lambda x, y: 1.0),
}
