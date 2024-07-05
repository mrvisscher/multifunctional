from bw2data.backends.proxies import Activity
from bw2data.backends.schema import ActivityDataset

from .node_classes import (
    Process,
    ReadOnlyProduct,
    ProcessProduct
)


def multifunctional_node_dispatcher(node_obj: ActivityDataset) -> Activity:
    """Dispatch the correct node class depending on node_obj attributes."""
    if node_obj.type == "process":
        return Process(document=node_obj)
    elif node_obj.type == "product":
        return ReadOnlyProduct(document=node_obj)
    elif node_obj.type == "process-product":
        return ProcessProduct(document=node_obj)
    else:
        print("Neither process nor product? Shouldn't happen...")
        return Activity(document=node_obj)
