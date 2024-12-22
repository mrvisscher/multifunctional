import logging
import warnings
from typing import Optional, Union

from bw2data import databases, get_node, labels
from bw2data.backends.proxies import Activity
from loguru import logger

from .edge_classes import ReadOnlyExchanges
from .errors import NoAllocationNeeded
from .utils import (
    purge_expired_linked_readonly_processes,
    set_correct_process_type,
    update_datasets_from_allocation_results,
)


class Process(Activity):

    def save(self, *args, **kwargs):
        set_correct_process_type(self)
        purge_expired_linked_readonly_processes(self)
        super().save(*args, **kwargs)

    def new_product(self, **kwargs):
        kwargs["type"] = "product"
        fn = Function(**kwargs)
        fn.save()
        fn.processor = self  # should this be part of the save function instead?
        return fn

    def new_reduct(self, **kwargs):
        kwargs["type"] = "product"
        fn = Function(**kwargs)
        fn.save()
        fn.processor = self
        return fn

    def functions(self):
        return [self]

    @property
    def functional(self) -> bool:
        return len(list(self.functions())) > 0

    @property
    def multifunctional(self) -> bool:
        return len(list(self.functions())) > 1

    def allocate(
        self, strategy_label: Optional[str] = None, products_as_process: bool = False
    ) -> Union[None, NoAllocationNeeded]:
        if self.get("skip_allocation"):
            return NoAllocationNeeded()
        if not self.multifunctional:
            # Call save because we don't know if the process type should be changed
            self.save()
            return NoAllocationNeeded()

        from . import allocation_strategies

        if strategy_label is None:
            if self.get("default_allocation"):
                strategy_label = self.get("default_allocation")
            else:
                strategy_label = databases[self["database"]].get("default_allocation")

        if not strategy_label:
            raise ValueError(
                "Can't find `default_allocation` in input arguments, or process/database metadata."
            )
        if strategy_label not in allocation_strategies:
            raise KeyError(f"Given strategy label {strategy_label} not in `allocation_strategies`")

        logger.debug(
            "Allocating {p} (id: {i}) with strategy {s}",
            p=repr(self),
            i=self.id,
            s=strategy_label,
        )

        allocated_data = allocation_strategies[strategy_label](self)
        update_datasets_from_allocation_results(allocated_data)


class Function(Activity):
    """Can be type 'product' or 'reduct'"""
    def __init__(self, document=None, **kwargs):
        super().__init__(document, **kwargs)

    @property
    def orphaned(self) -> bool:
        """Orphaned functions have no processor and will create a non-square matrix"""
        return bool(self.processor)

    @property
    def processor(self) -> Process | None:
        """Return the single process with the production/reduction flow"""
        return None

    @processor.setter
    def processor(self, process: Process):
        """Set the processor for this production flow"""
        if self.processor:
            logging.info(f"Changing processor for {self} from {self.processor} to {process}")

        # create production or reduction flow based on type using super().new_edge()
        # set output to the processor
        return

    def substitute(self):
        """Can I think of a way to substitute here?"""
        pass

    def new_edge(self, **kwargs):
        """Impossible for a Function"""
        pass


class ReadOnlyProcess(Activity):
    def __str__(self):
        base = super().__str__()
        return f"Read-only allocated process: {base}"

    def __setitem__(self, key, value):
        raise NotImplementedError(
            "This node is read only. Update the corresponding multifunctional process."
        )

    @property
    def parent(self):
        """Return the `MultifunctionalProcess` which generated this node object"""
        return get_node(
            database=self["mf_parent_key"][0],
            code=self["mf_parent_key"][1],
        )

    def save(self):
        self._data["type"] = "readonly_process"
        if not self.get("mf_parent_key"):
            raise ValueError("Must specify `mf_parent_key`")
        super().save()

    def copy(self, *args, **kwargs):
        raise NotImplementedError(
            "This node is read only. Update the corresponding multifunctional process."
        )

    def new_edge(self, **kwargs):
        raise NotImplementedError(
            "This node is read only. Update the corresponding multifunctional process."
        )

    def exchanges(self, exchanges_class=None):
        if exchanges_class is not None:
            warnings.warn("`exchanges_class` argument ignored; must be `ReadOnlyExchanges`")
        return super().exchanges(exchanges_class=ReadOnlyExchanges)

    def technosphere(self, exchanges_class=None):
        if exchanges_class is not None:
            warnings.warn("`exchanges_class` argument ignored; must be `ReadOnlyExchanges`")
        return super().technosphere(exchanges_class=ReadOnlyExchanges)

    def biosphere(self, exchanges_class=None):
        if exchanges_class is not None:
            warnings.warn("`exchanges_class` argument ignored; must be `ReadOnlyExchanges`")
        return super().biosphere(exchanges_class=ReadOnlyExchanges)

    def production(self, include_substitution=False, exchanges_class=None):
        if exchanges_class is not None:
            warnings.warn("`exchanges_class` argument ignored; must be `ReadOnlyExchanges`")
        return super().production(
            include_substitution=include_substitution, exchanges_class=ReadOnlyExchanges
        )

    def substitution(self, exchanges_class=None):
        if exchanges_class is not None:
            warnings.warn("`exchanges_class` argument ignored; must be `ReadOnlyExchanges`")
        return super().substitution(exchanges_class=ReadOnlyExchanges)

    def upstream(self, kinds=labels.technosphere_negative_edge_types, exchanges_class=None):
        if exchanges_class is not None:
            warnings.warn("`exchanges_class` argument ignored; must be `ReadOnlyExchanges`")
        return super().upstream(kinds=kinds, exchanges_class=ReadOnlyExchanges)

