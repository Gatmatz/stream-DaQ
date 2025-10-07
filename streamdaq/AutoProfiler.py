from abc import ABC, abstractmethod
import pathway as pw


class AutoProfiler(ABC):
    """Abstract base class for all autoprofilers"""

    def __init__(self, **kwargs):
        """Initialize the autoprofiler with configuration parameters"""
        pass

    @abstractmethod
    def set_measures(self, data: pw.Table, time_column: str, instance: str) -> dict:
        """
        Set profiling measures based on the data structure

        Args:
            data: The pathway table to profile
            time_column: Name of the time column
            instance: Name of the instance column

        Returns:
            dict: Dictionary of profiling measures
        """
        pass

    @abstractmethod
    def consume_profiling_measures(self, profiling_stream: pw.Table) -> pw.Table:
        """
        Process the profiling measures and generate insights/alerts

        Args:
            profiling_stream: The stream containing profiling measures
        """
        pass