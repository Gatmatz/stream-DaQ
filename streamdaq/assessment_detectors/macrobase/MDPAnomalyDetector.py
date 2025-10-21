from typing import Tuple

from streamdaq.assessment_detectors.ThresholdDetector import ThresholdDetector
from streamdaq.assessment_detectors.macrobase.models.AdaptableDampedReservoir import AdaptableDampedReservoir


class MDPAnomalyDetector:
    # State objects
    anomaly_ADR = None
    anomaly_threshold = None
    window_count = None
    warmup_input = None
    warmup_period = None
    decay_period = None
    training_period = None
    percentile = None

    def __init__(self, config):
        self.anomaly_ADR = AdaptableDampedReservoir(capacity=config["sample_capacity"], seed=config["seed"], decay_rate=config["decay_rate"])
        self.anomaly_threshold = 0
        self.window_count = 0
        self.warmup_period = config["warmup_period"]
        self.warmup_input = []
        self.decay_period = 0
        self.training_period = config["training_period"]
        self.percentile = config["percentile"]

    def update_anomaly_threshold(self, percentile: float):
        norms = self.anomaly_ADR.get_sample()  # Should return a list of tuples
        if not norms:
            return 0  # or handle empty case as needed

        sorted_norms = sorted(norms, key=lambda x: x[1])
        index = int(percentile * len(norms))
        index = min(index, len(sorted_norms) - 1)  # Ensure index is in bounds
        current_threshold = sorted_norms[index][1]
        return current_threshold

    def anomaly_detection(self, window:Tuple) -> str:
        self.window_count += 1
        if self.window_count <= self.warmup_period:
            self.warmup_input.append(window)
            self.anomaly_ADR.insert(window)

            if self.window_count % (self.decay_period + 1) == 0:
                self.anomaly_ADR.advance_period()

            if self.window_count % (self.training_period + 1) == 0:
                self.anomaly_threshold = self.update_anomaly_threshold(self.percentile)

            if self.window_count == self.warmup_period:
                self.anomaly_threshold = self.update_anomaly_threshold(self.percentile)
                self.warmup_input.clear()

            return "unknown-warmup period"
        else:
            if self.window_count == self.warmup_period:
                self.anomaly_threshold = self.update_anomaly_threshold(self.percentile)

            self.anomaly_ADR.insert(window)

            if window[1] > self.anomaly_threshold:
                return "defective"
            else:
                return "normal"