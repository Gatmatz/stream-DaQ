from streamdaq.assessment_detectors.macrobase.models.AdaptableDampedReservoir import AdaptableDampedReservoir
from streamdaq.assessment_detectors.macrobase.models.MADScorer import MADScorer

class MADTrainer:
    # State objects
    train_reservoir = None
    trainer = None
    window_count = None
    warmup_period = None
    warmup_input = None
    decay_period = None
    training_period = None
    decay_rate = None

    def __init__(self, config):
        self.train_reservoir = AdaptableDampedReservoir(capacity=config["sample_capacity"], seed=config["seed"], decay_rate=config["decay_rate"])
        self.trainer  = MADScorer()
        self.window_count = 0
        self.warmup_period = config["warmup_period"]
        self.warmup_input = []
        self.decay_period = config["decay_period"]
        self.training_period = config["training_period"]

    def mad_train(self, window):
        self.window_count += 1
        if self.window_count <= self.warmup_period:
            self.warmup_input.append(window)
            self.train_reservoir.insert(window)

            if self.window_count % (self.training_period + 1) == 0:
                self.trainer.train(self.train_reservoir.get_sample())

            if self.window_count % (self.decay_period + 1) == 0:
                self.train_reservoir.advance_period()

            if self.window_count == self.warmup_period:
                self.trainer.train(self.train_reservoir.get_sample())
                self.warmup_input.clear()

            return window, self.trainer.score(window)
        else:
            if self.window_count % (self.training_period + 1) == 0:
                self.trainer.train(self.train_reservoir.get_sample())

            if self.window_count % (self.decay_period + 1) == 0:
                self.train_reservoir.advance_period()

            self.train_reservoir.insert(window)
            return window, self.trainer.score(window)