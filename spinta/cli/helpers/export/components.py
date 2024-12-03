from collections import defaultdict

import tqdm

from spinta.components import Model


class CounterManager:
    enabled: bool
    total_counter: tqdm.tqdm
    counters: dict[str, dict[str, tqdm.tqdm]]
    totals: dict[str, int]

    def __init__(self, enabled: bool, totals: dict):
        self.enabled = enabled
        self.totals = totals
        self.counters = defaultdict(dict)

        total = sum([value for value in totals.values()])

        self.total_counter = tqdm.tqdm(
            desc="Export total",
            total=total,
            ascii=True,
        ) if enabled else None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.enabled:
            self.close_all()

    def add_counter(self, model: Model, desc: str):
        if not self.enabled:
            return

        key = model.model_type()
        self.counters[key][desc] = tqdm.tqdm(
            desc=desc,
            total=self.totals[key],
            ascii=True
        )

    def update_total(self, value: int):
        if not self.enabled:
            return

        self.total_counter.update(value)

    def update_specific(self, model: Model, key: str, value: int):
        if not self.enabled:
            return

        self.counters[model.model_type()][key].update(value)

    def close_all(self):
        if not self.enabled:
            return

        self.total_counter.close()
        for counters in self.counters.values():
            for counter in counters.values():
                counter.close()

    def close_model(self, model: Model):
        if not self.enabled:
            return

        for counter in self.counters[model.model_type()].values():
            counter.close()
