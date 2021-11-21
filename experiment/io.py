from typing import Union, BinaryIO

import pandas as pd

from experiment.data import ExperimentMetadata, Plate, Experiment
from tecan.io import TecanReader


class MetadataReader:

    @classmethod
    def read(cls, file):
        raw = pd.read_excel(file, header=0)
        return ExperimentMetadata(raw)


class ExperimentReader:

    @classmethod
    def read(cls, plates, metadata):
        _plates = [TecanReader.read(file) for file in plates]
        _metadata = MetadataReader.read(metadata)
        return Experiment(_plates, _metadata)

    @classmethod
    def add_plates(cls, experiment, plates, metadata):
        _plates = [TecanReader.read(file) for file in plates]
        _metadata = MetadataReader.read(metadata)
        experiment.add_plates(_plates, _metadata)

    @classmethod
    def add_plate(cls, experiment, plate, metadata):
        _plate = TecanReader.read(plate)
        _metadata = MetadataReader.read(metadata)
        experiment.add_plates(_plate, _metadata)
