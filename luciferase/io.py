import datetime
import openpyxl
import pandas as pd

from luciferase.data import TecanInfinitePlate, ExperimentMetadata, LuciferaseExperiment, DualLuciferaseExperiment


class TecanReader:

    TS_FORMAT = '%m/%d/%Y %I:%M:%S %p'

    @classmethod
    def read(cls, file):
        if isinstance(file, list):
            wells = None
            metadata = None
            for f in file:
                p = cls.read(f)
                if wells is None:
                    wells = p.wells
                else:
                    wells = wells.append(p.wells.drop(wells.index, errors='ignore')).sort_index()
                if metadata is None:
                    metadata = p.metadata
                else:
                    for k, v in metadata.items():
                        if v is None:
                            metadata[k] = p.metadata.get(k, None)
            data = wells.reset_index().pivot(index='row', columns='column', values='value')
        else:
            wb = openpyxl.load_workbook(file, read_only=True)
            ws = wb.active
            start_row = cls._get_data_row(ws)
            date = cls._get_date(ws)
            time = cls._get_time(ws)
            timestamp = datetime.datetime.strptime(date.strftime('%m/%d/%Y ') + time, cls.TS_FORMAT)
            instrument = cls._get_instrument(ws)
            start_ts = cls._get_start_ts(ws)
            end_ts = cls._get_end_ts(ws)
            raw = pd.read_excel(file, skiprows=start_row - 1, header=0, index_col=0)
            data = raw.loc[
                [i for i in raw.index if str(i).isupper() and len(i) == 1],
                [c for c in raw.columns if not str(c).startswith('Unnamed: ')]].apply(pd.to_numeric, errors='coerce')

            metadata = {
                'timestamp': timestamp,
                'start': start_ts,
                'end': end_ts,
                'instrument': instrument
            }

        return TecanInfinitePlate(data, metadata)

    @staticmethod
    def _get_data_row(ws):
        for row in ws.iter_rows(min_col=1, max_col=1):
            for cell in row:
                if cell.value == "<>":
                    return cell.row
        return None

    @staticmethod
    def _get_date(ws):
        for row in ws.iter_rows(min_col=1, max_col=1):
            for cell in row:
                if cell.value == "Date:":
                    return ws.cell(row=cell.row, column=2).value
        return None

    @staticmethod
    def _get_time(ws):
        for row in ws.iter_rows(min_col=1, max_col=1):
            for cell in row:
                if cell.value == "Time:":
                    return ws.cell(row=cell.row, column=2).value
        return None

    @staticmethod
    def _get_instrument(ws):
        for row in ws.iter_rows(min_col=1, max_col=1):
            for cell in row:
                if str(cell.value).startswith('Device: '):
                    return cell.value.replace('Device: ', '')
        return None

    @classmethod
    def _get_start_ts(cls, ws):
        for row in ws.iter_rows(min_col=1, max_col=1):
            for cell in row:
                if cell.value == 'Start Time:':
                    value = ws.cell(row=cell.row, column=2).value
                    return datetime.datetime.strptime(value, cls.TS_FORMAT) if value is not None else value
        return None

    @classmethod
    def _get_end_ts(cls, ws):
        for row in ws.iter_rows(min_col=1, max_col=1):
            for cell in row:
                if cell.value == 'End Time:':
                    value = ws.cell(row=cell.row, column=2).value
                    return datetime.datetime.strptime(value, cls.TS_FORMAT) if value is not None else value
        return None


class MetadataReader:

    @classmethod
    def read(cls, file):
        raw = pd.read_excel(file, header=0)
        return ExperimentMetadata(raw)


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
