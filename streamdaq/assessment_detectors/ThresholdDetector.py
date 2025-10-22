from abc import ABC, abstractmethod
import pathway as pw


class ThresholdDetector(ABC):
    """Abstract base class for all assessment detectors"""

    def __init__(self, **kwargs):
        """Initialize the assessment detector with configuration parameters"""
        pass

    @abstractmethod
    def assess(self, data: pw.Table, column: str) -> str:
        """
        Assess the data in the specified column of the table.
        :param column: column to assess
        """
        pass