import numpy as np
from collections import deque
from sklearn.cluster import KMeans

from streamdaq.assessment_detectors.ThresholdDetector import ThresholdDetector

class KMeansDetector(ThresholdDetector):
    """Online assessment detector using KMeans clustering algorithm."""

    def __init__(self, window_size: int = 10, warmup_period: int = 10, n_clusters: int = 2, distance_threshold: float = 2.0):
        super().__init__()
        self.window_size = window_size
        self.warmup_period = warmup_period
        self.n_clusters = n_clusters
        self.distance_threshold = distance_threshold
        self.values = deque(maxlen=window_size)

    def check_window(self, value) -> str:
        # Add new value to window
        self.values.append(value)
        # Not enough data to fit KMeans
        if len(self.values) < max(self.warmup_period, self.n_clusters):
            return "warmup"
        # Fit KMeans
        X = np.array(self.values).reshape(-1, 1)
        kmeans = KMeans(n_clusters=self.n_clusters, n_init="auto")
        kmeans.fit(X)
        # Find cluster center for current value
        current_value = np.array([[value]])
        cluster_label = kmeans.predict(current_value)[0]
        center = kmeans.cluster_centers_[cluster_label][0]
        # Compute distance to center
        distance = abs(value - center)
        # Outlier detection
        if distance > self.distance_threshold:
            return "outlier"
        return "inlier"