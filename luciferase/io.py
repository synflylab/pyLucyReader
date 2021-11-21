from tecan.io import TecanReader
from experiment.io import MetadataReader
from luciferase.data import LuciferaseExperiment, DualLuciferaseExperiment


class LuciferaseExperimentReader:

    @classmethod
    def read(cls, metadata_file, plate_files):
        plates = [TecanReader.read(file) for file in plate_files]
        metadata = MetadataReader.read(metadata_file)

        return LuciferaseExperiment(metadata, plates)


class DualLuciferaseExperimentReader:

    @classmethod
    def read(cls, metadata_file, firefly_files, renilla_files):
        firefly = [TecanReader.read(file) for file in firefly_files]
        renilla = [TecanReader.read(file) for file in renilla_files]
        metadata = MetadataReader.read(metadata_file)

        return DualLuciferaseExperiment(metadata, firefly, renilla)
