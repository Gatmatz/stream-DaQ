from typing import Tuple

import pathway as pw

from streamdaq.assessment_detectors.ThresholdDetector import ThresholdDetector
from streamdaq.assessment_detectors.macrobase.MADTrainer import MADTrainer
from streamdaq.assessment_detectors.macrobase.MDPAnomalyDetector import MDPAnomalyDetector


class MDPDetector(ThresholdDetector):
    mad_trainer = None
    anomaly_detector = None

    def __init__(self, config):
        self.config = config
        self.mad_trainer = MADTrainer(config)
        self.anomaly_detector = MDPAnomalyDetector(config)

    def assess(self, data: pw.Table, column: str) -> str:
        mad_attach = pw.apply_with_type(
            self.mad_trainer.mad_train, Tuple, data[column]
        )

        assessment = pw.apply_with_type(
            self.anomaly_detector.anomaly_detection, str, mad_attach
        )
        return assessment
