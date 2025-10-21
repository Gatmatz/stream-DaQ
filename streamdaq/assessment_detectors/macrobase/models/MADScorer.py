import math
from typing import List
import pathway as pw

class MADScorer:
    def __init__(self) -> None:
        self.median: float = 0.0
        self.MAD: float = 0.0
        self.trimmedMeanFallback: float = 0.05
        # https://en.wikipedia.org/wiki/Median_absolute_deviation#Relation_to_standard_deviation
        self.MAD_TO_ZSCORE_COEFFICIENT: float = 1.4826

    def train(self, data) -> None:
        if not data:
            raise ValueError("data must not be empty")

        metrics = [float(r) for r in data]
        metrics.sort()
        n = len(metrics)

        if n % 2 == 0:
            self.median = (metrics[n // 2 - 1] + metrics[n // 2]) / 2.0
        else:
            self.median = metrics[n // 2]

        residuals = [abs(x - self.median) for x in metrics]
        residuals.sort()

        if n % 2 == 0:
            self.MAD = (residuals[n // 2 - 1] + residuals[n // 2]) / 2.0
        else:
            self.MAD = residuals[n // 2]

        if self.MAD == 0:
            lower = int(len(residuals) * self.trimmedMeanFallback)
            upper = int(len(residuals) * (1 - self.trimmedMeanFallback))
            if upper <= lower:
                # fallback to a small positive value to avoid division by zero
                self.MAD = 1e-12
            else:
                s = sum(residuals[lower:upper])
                self.MAD = s / (upper - lower)

    def score(self, record) -> float:
        point = float(record)
        if self.MAD == 0:
            # return large value if MAD is zero (shouldn't happen after train)
            return float("inf")
        return abs(point - self.median) / self.MAD

    def getZScoreEquivalent(self, zscore: float) -> float:
        return zscore / self.MAD_TO_ZSCORE_COEFFICIENT