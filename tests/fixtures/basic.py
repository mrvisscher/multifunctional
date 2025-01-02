DATA = {
    ("basic", "a"): {
        "name": "flow - a",
        "code": "a",
        "unit": "kg",
        "type": "emission",
        "categories": ("air",),
    },
    ("basic", "1"): {
        "name": "process - 1",
        "code": "1",
        "location": "first",
        "type": "multifunctional",
        "exchanges": [
            {
                "type": "production",
                "amount": 4,
                "input": ("basic", "2")
            },
            {
                "type": "production",
                "amount": 6,
                "input": ("basic", "3")
            },
            {
                "type": "biosphere",
                "name": "flow - a",
                "amount": 10,
                "input": ("basic", "a"),
            },
        ],
    },
    ("basic", "2"): {
            "name": "product - 1",
            "code": "2",
            "location": "first",
            "type": "product",
            "unit": "kg",
            "properties": {
                "price": 7,
                "mass": 6,
                "manual_allocation": 2,
            },
        },
    ("basic", "3"): {
            "name": "product - 2",
            "code": "3",
            "location": "first",
            "type": "product",
            "unit": "megajoule",
            "properties": {
                "price": 12,
                "mass": 4,
                "manual_allocation": 8,
            },
        },
}
