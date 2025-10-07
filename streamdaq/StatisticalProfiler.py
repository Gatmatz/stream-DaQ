from collections import defaultdict, deque

import pathway as pw
from streamdaq.DaQMeasures import DaQMeasures
from streamdaq.AutoProfiler import AutoProfiler

class StatisticalProfiler(AutoProfiler):
    def __init__(self, window_size=10):
        self.nof_summaries = 0
        self.window_size = window_size
        self.rolling_history = defaultdict(lambda: deque(maxlen=window_size))
        self.rolling_means = {}

    def set_measures(self, data, time_column, instance):
        profiling_measures = {}

        # Identify numeric columns
        numeric_columns = []

        for col_name in data.column_names():
            if col_name not in [time_column, instance, '_validation_metadata']:
                try:
                    test_col = pw.cast(float, data[col_name])
                    numeric_columns.append(col_name)
                except Exception:
                    continue

        for col_name in numeric_columns:
            profiling_measures[f"{col_name}_min_prof"] = DaQMeasures.min(col_name)
            profiling_measures[f"{col_name}_max_prof"] = DaQMeasures.max(col_name)
            profiling_measures[f"{col_name}_mean_prof"] = DaQMeasures.mean(col_name)
            profiling_measures[f"{col_name}_missing_count_prof"] = DaQMeasures.missing_count(col_name)
            profiling_measures[f"{col_name}_total_count_prof_prof"] = DaQMeasures.count(col_name)
            profiling_measures[f"{col_name}_missing_percentage_prof"] = DaQMeasures.missing_fraction(col_name, precision=3)

        self.nof_summaries = int(len(profiling_measures) / len(numeric_columns) if numeric_columns else 0)

        # todo: Identify categorical columns and implement peculiarity measure
        # categorical_columns = []
        # for col_name in data.column_names():

        return profiling_measures

    def update_rolling_means(self, current_measures):
        """Update rolling means with current measure values"""
        for measure_name, current_value in current_measures.items():
            # Add current value to rolling history
            self.rolling_history[measure_name].append(current_value)

            # Calculate rolling mean
            if len(self.rolling_history[measure_name]) > 0:
                self.rolling_means[measure_name] = sum(self.rolling_history[measure_name]) / len(
                    self.rolling_history[measure_name])

    def compute_anomaly_score(self, current_measures):
        """Compute anomaly score based on difference from rolling mean"""
        if not self.rolling_means:
            return 0.0

        differences = []

        for measure_name, current_value in current_measures.items():
            if measure_name in self.rolling_means:
                rolling_mean = self.rolling_means[measure_name]
                # Handle division by zero for percentage difference
                if rolling_mean != 0:
                    diff = abs(current_value - rolling_mean) / abs(rolling_mean)
                else:
                    diff = abs(current_value) if current_value != 0 else 0
                differences.append(diff)

        # Return mean of all differences as final anomaly score
        return sum(differences) / len(differences) if differences else 0.0

    def consume_profiling_measures(self, profiling_stream):
        pw.debug.compute_and_print(profiling_stream)

        # todo: Extract current profiling measures from the stream
        current_values = {}
        for row in profiling_stream:
            for col_name in profiling_stream.column_names():
                if col_name not in ['__time__', '__diff__']:  # Skip pathway metadata columns
                    current_values[col_name] = row[col_name]

        print(f"Current profiling measures: {current_values}")
        # Update rolling means
        self.update_rolling_means(current_values)

        # Compute anomaly score
        anomaly_score = self.compute_anomaly_score(current_values)

        print(f"Current anomaly score: {anomaly_score:.4f}")

        return anomaly_score