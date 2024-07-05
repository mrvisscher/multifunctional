from typing import Optional, Union
from uuid import uuid4
from copy import deepcopy

from bw2data import databases, labels
from bw2data.backends.proxies import Activity, Exchange
from loguru import logger

from .edge_classes import ReadOnlyExchanges, ReadOnlyExchange
from .errors import NoAllocationNeeded
from .utils import update_products_from_allocation_results


class Process(Activity):
    def products(self):
        return [exc.output for exc in self.upstream(kinds=["production"])]

    def multifunctional(self) -> bool:
        return len(self.products()) > 1

    def valid(self, why=False):
        if super().valid():
            errors = []
        else:
            valid, errors = super().valid(True)

        # should have at least one product associated with it
        products = self.products()
        if not len(products) > 0:
            errors.append("Process should have at least one product associated with it")

        if self in products:
            errors.append("Process cannot produce itself, associate a product with this process")

        if why:
            return len(errors) == 0, errors
        else:
            return len(errors) == 0

    def new_product(self, name, code=None, amount=1, **kwargs):
        attributes: dict = deepcopy(self._data)

        attributes.update(kwargs)
        attributes["name"] = name
        attributes["code"] = code or uuid4().hex
        attributes["type"] = "product"

        product = ReadOnlyProduct(**attributes)
        product.save()

        exc = Exchange(
            input=self.key,
            output=product.key,
            type="production",
            amount=amount
        )
        exc.save()

        self.allocate()

        return product

    def allocate(
            self, strategy_label: Optional[str] = None
    ) -> Union[None, NoAllocationNeeded]:
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
        elif strategy_label not in allocation_strategies:
            raise KeyError(
                f"Given strategy label {strategy_label} not in `allocation_strategies`"
            )

        if self.get("skip_allocation"):
            return NoAllocationNeeded

        logger.debug(
            "Allocating {p} (id: {i}) with strategy {s}",
            p=repr(self),
            i=self.id,
            s=strategy_label,
        )

        allocated_data = allocation_strategies[strategy_label](self)
        update_products_from_allocation_results(allocated_data)


class ReadOnlyProduct(Activity):
    def __str__(self):
        base = super().__str__()
        return f"Read-only allocated process: {base}"

    def __setitem__(self, key, value):
        raise NotImplementedError(
            "This node is read only. Update the corresponding multifunctional process."
        )

    @property
    def parent(self):
        """Return the `Process` which supplies the production exchange to this product"""
        return self.production()[0].input

    def copy(self, *args, **kwargs):
        raise NotImplementedError(
            "This node is read only. Update the corresponding multifunctional process."
        )

    def new_edge(self, **kwargs):
        raise NotImplementedError(
            "This node is read only. Update the corresponding multifunctional process."
        )

    def delete(self):
        raise NotImplementedError(
            "This node is read only. Update the corresponding multifunctional process."
        )

    def exchanges(self):
        return super().exchanges(exchanges_class=ReadOnlyExchanges)

    def technosphere(self):
        return super().technosphere(exchanges_class=ReadOnlyExchanges)

    def biosphere(self):
        return super().biosphere(exchanges_class=ReadOnlyExchanges)

    def production(self, include_substitution=False):
        return super().production(
            include_substitution=include_substitution, exchanges_class=ReadOnlyExchanges
        )

    def substitution(self):
        return super().substitution(exchanges_class=ReadOnlyExchanges)

    def upstream(self, kinds=labels.technosphere_negative_edge_types):
        return super().upstream(kinds=kinds, exchanges_class=ReadOnlyExchanges)


class ProcessProduct(Activity):
    """
    Node that is both a process and a product, aka Process with a reference product, aka. a Chimaera.

    These cannot be multifunctional, but they can be converted to a Process-Product link
    """
