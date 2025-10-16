import numpy as np
from collections import deque

from streamdaq.assessment_detectors.ThresholdDetector import ThresholdDetector


class OnlineNormalThreshold(ThresholdDetector):
    """Online assessment detector using running statistics."""

    def __init__(self, window_size: int = 10, warmup_period: int = 10, threshold_method: str = "z_score"):
        super().__init__()
        self.window_size = window_size
        self.samples = deque(maxlen=window_size)
        self.sum = 0.0
        self.sum_sq = 0.0
        self.warmup_period = warmup_period
        self.threshold_method = threshold_method
        self.dynamic_threshold = 0.0
        self.windows_processed = 0

    def _calculate_z_score(self, value: float) -> float:
        """Calculate z-score for given value."""
        if len(self.samples) < self.warmup_period:
            return 0.0

        mean = self.sum / len(self.samples)
        variance = (self.sum_sq - (self.sum ** 2) / len(self.samples)) / (len(self.samples) - 1)
        std = np.sqrt(max(variance, 0))

        if std == 0:
            return 0.0

        return abs(value - mean) / std

    def _update_dynamic_threshold(self):
        """Update dynamic threshold based on current window values."""
        if len(self.samples) < self.warmup_period:
            return

        values = list(self.samples)

        if self.threshold_method == 'z_score':
            # For z-score, use a fixed threshold (e.g., 2 or 3 standard deviations)
            self.dynamic_threshold = 2.0
        elif self.threshold_method == 'percentile':
            self.dynamic_threshold = np.percentile(values, 95)
        elif self.threshold_method == 'iqr':
            q1 = np.percentile(values, 25)
            q3 = np.percentile(values, 75)
            iqr = q3 - q1
            self.dynamic_threshold = q3 + 1.5 * iqr

    def _is_anomalous(self, current_score, method_value) -> bool:
        """Determine if current score indicates an anomaly"""
        if self.windows_processed < self.warmup_period:
            return False

        if self.threshold_method == 'z_score':
            return current_score > self.dynamic_threshold
        else:
            return method_value > self.dynamic_threshold

    def _get_anomaly_severity(self, current_score, threshold) -> str:
        """Get anomaly severity level"""
        if threshold == 0:
            return "unknown"

        severity_ratio = current_score / threshold

        if severity_ratio > 2.0:
            return "critical"
        elif severity_ratio > 1.5:
            return "high"
        else:
            return "moderate"

    def check_window(self, window) -> str:
        """Add a value and return severity level."""
        # Convert to float if needed
        if not isinstance(window, (int, float)):
            try:
                window = float(window)
            except (ValueError, TypeError):
                return "invalid"

        # Check if in warmup period
        if self.windows_processed < self.warmup_period:
            # During warmup, just add the value without checking for outliers
            if len(self.samples) == self.window_size:
                old_value = self.samples[0]
                self.sum -= old_value
                self.sum_sq -= old_value ** 2

            self.samples.append(window)
            self.sum += window
            self.sum_sq += window ** 2
            self.windows_processed += 1
            self._update_dynamic_threshold()
            return "normal"

        # Calculate anomaly score based on current samples (before adding new value)
        if self.threshold_method == 'z_score':
            score = self._calculate_z_score(window)
            is_outlier = self._is_anomalous(score, window)
            threshold = self.dynamic_threshold
        elif self.threshold_method == 'percentile':
            score = window
            is_outlier = window > self.dynamic_threshold
            threshold = self.dynamic_threshold
        elif self.threshold_method == 'iqr':
            score = window
            is_outlier = window > self.dynamic_threshold
            threshold = self.dynamic_threshold
        else:
            return "invalid"

        # Now update statistics and add new value
        if len(self.samples) == self.window_size:
            old_value = self.samples[0]
            self.sum -= old_value
            self.sum_sq -= old_value ** 2

        self.samples.append(window)
        self.sum += window
        self.sum_sq += window ** 2
        self.windows_processed += 1

        # Update dynamic threshold after adding new value
        self._update_dynamic_threshold()

        if not is_outlier:
            return "normal"

        # Determine severity
        return self._get_anomaly_severity(score, threshold)