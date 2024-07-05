from bw2data.backends.proxies import Exchange, Exchanges


def update_products_from_allocation_results(data: dict):

    for key, ds in data.items():
        # first delete the old exchanges
        Exchanges(key, ["technosphere", "biosphere"]).delete()

        # replace with the new exchanges
        for exchange in ds["exchanges"]:
            Exchange(**exchange).save()

