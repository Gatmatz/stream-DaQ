from abc import ABC, abstractmethod
import pathway as pw


class ThresholdDetector(ABC):
    """Abstract base class for all assessment detectors"""

    def __init__(self, **kwargs):
        """Initialize the assessment detector with configuration parameters"""
        pass

    @abstractmethod
    def check_window(self, window: int | float) -> str:
        """
        Driver function to check if the measure in the window breaches a dynamic threshold.
        :param window: window of a column of a specific window
        :return: assessment result
        """
        pass