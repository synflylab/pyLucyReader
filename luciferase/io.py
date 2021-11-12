import datetime
from typing import Iterable, Union

import openpyxl
import numpy as np
import pandas as pd

from luciferase.data import ExperimentMetadata, LuciferaseExperiment, DualLuciferaseExperiment
from tecan.io import LegacyTecanReader


class MetadataReader:

    @classmethod
    def read(cls, file):
        raw = pd.read_excel(file, header=0)
        return ExperimentMetadata(raw)


class LuciferaseExperimentReader:

    @classmethod
    def read(cls, metadata_file, plate_files):
        plates = [LegacyTecanReader.read(file) for file in plate_files]
        metadata = MetadataReader.read(metadata_file)

        return LuciferaseExperiment(metadata, plates)


class DualLuciferaseExperimentReader:

    @classmethod
    def read(cls, metadata_file, firefly_files, renilla_files):
        firefly = [LegacyTecanReader.read(file) for file in firefly_files]
        renilla = [LegacyTecanReader.read(file) for file in renilla_files]
        metadata = MetadataReader.read(metadata_file)

        return DualLuciferaseExperiment(metadata, firefly, renilla)
