import math
from collections import deque
from typing import Deque, Tuple, Optional, List
import random
import heapq
from dataclasses import dataclass

from testing.models.JavaRandom import JavaRandom


@dataclass(order=True)
class OverweightItem:
    sort_index: float
    record: object

    def __init__(self, record, weight):
        self.sort_index = -weight  # negate for max-heap
        self.record = record


class AdaptableDampedReservoir:
    def __init__(self, capacity: int = 1024, seed: Optional[int] = 0):
        if capacity <= 0:
            raise ValueError("capacity must be > 0")

        self.capacity: int = capacity
        self._rand = JavaRandom(seed)

        self._reservoir: Deque[float] = deque(maxlen=capacity)
        self._running_count: float = 0
        self.overweight_items: List[OverweightItem] = []

    def insert(self, record: float, weight: float) -> None:
        """
        Add a sample to the reservoir.
        If timestamp is None an internal monotonic index will be used.
        """
        self._running_count += weight

        self._update_overweight_items()

        if len(self._reservoir) < self.capacity:
            self._reservoir.append(record)
        else:
            probability_of_insertion = len(self._reservoir) * weight / self._running_count
            ran = self._rand.nextDouble()
            if probability_of_insertion > 1.0:
                item = OverweightItem(record, weight)
                heapq.heappush(self.overweight_items, item)
            else:
                if ran < probability_of_insertion:
                    replace_index = self._rand.nextInt(len(self._reservoir))
                    self._reservoir[replace_index] = record

    def _update_overweight_items(self) -> None:
        while self.overweight_items:
            ow = self.overweight_items[0]
            ow_weight = -ow.sort_index
            if self.capacity * ow_weight / self._running_count <= 1:
                heapq.heappop(self.overweight_items)
                self.insert(ow.record, ow_weight)
            else:
                return

    def decay_weights(self, decay: float) -> None:
        """
        Decay the weights of all samples in the reservoir.
        """
        if decay < 0:
            raise ValueError("decay must be >= 0")

        self._running_count *= decay

        updated_overweight_items = []
        for item in self.overweight_items:
            weight = -item.sort_index
            updated_item = OverweightItem(item.record, weight * decay)
            updated_overweight_items.append(updated_item)

        self.overweight_items.clear()
        for item in updated_overweight_items:
            heapq.heappush(self.overweight_items, item)

    def get_sample(self) -> List[float]:
        """
        Get the current sample from the reservoir.
        """
        self._update_overweight_items()

        if self.overweight_items:
            overweight_list = [item.record for item in self.overweight_items]

            assert len(overweight_list) <= self.capacity

            shuffled_reservoir = list(self._reservoir)
            self._rand.shuffle(shuffled_reservoir)
            remaining_records = self.capacity - len(overweight_list)

            return overweight_list + shuffled_reservoir[:remaining_records]
        else:
            return list(self._reservoir)
