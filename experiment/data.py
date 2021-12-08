from __future__ import annotations

from datetime import datetime
from typing import Iterable, List, Union

import pandas as pd
import warnings

from base.accessors import AssayAccessor as AbstractAssayAccessor
from base.accessors import TimeAccessor as AbstractTimeAccessor
from tecan.data import Plate, PlateMetadata, AssayCollection, InstrumentMetadata, AbstractAssay, AssayMetadata, \
                       GenericMetadata


class CorrectedMetadata(GenericMetadata):

    def __new__(cls, *args, **kwargs) -> GenericMetadata:
        if args and isinstance(args[0], Iterable):
            metadata = args[0]
        elif 'metadata' in kwargs:
            metadata = kwargs['metadata']
        else:
            raise ValueError('No metadata provided')

        metadata_type = None
        for m in metadata:
            if not isinstance(m, GenericMetadata):
                raise TypeError('Metadata must be instances of GenericMetadata')
            if metadata_type is not None and m.__class__ != metadata_type:
                raise TypeError('All metadata must be of the same type')
            metadata_type = m.__class__

        oldmeta = [dict(m) for m in metadata]
        newmeta = {}
        for key in set([k for m in oldmeta for k in m.keys()]):
            newmeta[key] = [m.get(key) for m in oldmeta]
            if len(set(newmeta[key])) == 1:
                newmeta[key] = newmeta[key][0]

        return metadata_type(newmeta)


class CorrectedAssay(AbstractAssay):

    def __new__(cls, *args, **kwargs) -> CorrectedAssay:
        if args and isinstance(args[0], Iterable):
            assays = args[0]
        elif 'assays' in kwargs:
            assays = kwargs['assays']
        else:
            raise ValueError('No assays provided')

        assay_type = None
        for assay in assays:
            if not isinstance(assay, AbstractAssay):
                raise TypeError('Assays must be instances of AbstractAssay')
            if assay_type is not None and assay.__class__ != assay_type:
                raise TypeError('All assays must be of the same type')
            assay_type = assay.__class__

        implementation = type('Corrected' + assay_type.__name__, (CorrectedAssay, assay_type), {})
        # noinspection PyTypeChecker
        return super().__new__(implementation)

    # noinspection PyMissingConstructor
    def __init__(self, assays: Iterable[AbstractAssay], update=False) -> None:
        self._assays = list(assays)
        self._data = None
        self._metadata = CorrectedMetadata([a.metadata for a in self._assays])
        self._update = update

    def __repr__(self) -> str:
        return str(self.__class__.__name__ + '(name=' + self.name + ', mode=' + self.metadata.mode + ')')

    def _cache_data(self):
        self._data = self._assays[0].raw
        for assay in self._assays[1:]:
            self._data = self._data.append(assay.raw.drop(self._data.index, errors='ignore'))
            if self._update:
                self._data.update(assay.data)
        self._data.sort_index(inplace=True)

    @property
    def data(self) -> pd.DataFrame:
        if self._data is None:
            self._cache_data()
        return super().data

    @property
    def raw(self) -> pd.DataFrame:
        if self._data is None:
            self._cache_data()
        return self._data

    @property
    def name(self) -> str:
        return self._assays[0].name

    @property
    def metadata(self) -> AssayMetadata:
        return self._metadata


class CorrectedPlate(Plate):

    # noinspection PyMissingConstructor
    def __init__(self, plates: Iterable[Plate]):
        self._plates = plates
        self._assays = AssayCollection(CorrectedAssay(a) for a in zip(*(p.assays for p in plates)))
        self._metadata = CorrectedMetadata([p.metadata for p in self._plates])
        self._instrument = CorrectedMetadata([p.instrument for p in self._plates])

    @property
    def timestamp(self) -> datetime:
        return self._instrument.session_start

    @property
    def start(self) -> datetime:
        return self._metadata.start

    @property
    def end(self) -> datetime:
        return self._metadata.end


class ExperimentMetadata(pd.DataFrame):

    def __init__(self, raw: pd.DataFrame) -> None:
        raw.columns = [c.lower() for c in raw.columns]
        if 'well' not in raw.columns:
            wells = [c for c in raw.columns if 'well' in c]
            columns = [c for c in raw.columns if c not in wells]
            raw = raw.melt(id_vars=columns, value_vars=wells, var_name='replica', value_name='well') \
                     .drop('replica', axis='columns')
        raw = raw.join(raw['well'].str.extract('([A-Z]+)([0-9]+)').rename({0: 'row', 1: 'column'}, axis='columns')) \
                 .drop(raw.loc[raw['well'].isna()].index)\
                 .drop('well', axis='columns')
        raw['plate'] = raw['plate'].astype(int)
        raw['column'] = raw['column'].astype(int)
        raw = raw.set_index(['plate', 'row', 'column']).sort_index()

        idx = raw.index.levels[0]
        if not (idx.is_monotonic & idx.is_unique & (idx.size == idx.max())):
            missing = [str(i) for i in set(range(1, idx.max() + 1)) - set(raw.index.get_level_values(0).unique())]
            warnings.warn('Metadata for plate' + ('s ' if len(missing) > 1 else ' ') +
                          ', '.join(missing) + ' is missing', Warning)

        super().__init__(raw)


class AssayAccessor(AbstractAssayAccessor):

    def __init__(self, reads: pd.DataFrame) -> None:
        self._reads = reads

    @property
    def _assay_df(self) -> pd.DataFrame:
        return self._reads


class TimeAccessor(AbstractTimeAccessor):

    def __init__(self, reads: pd.DataFrame) -> None:
        self._reads = reads

    @property
    def _time_df(self) -> pd.DataFrame:
        return self._reads


class Experiment:

    def __init__(self, plates: Union[Plate, Iterable[Plate]], metadata: ExperimentMetadata) -> None:
        plates = [plates] if isinstance(plates, Plate) else list(plates)
        self._verify(plates, metadata)
        self._plates = plates
        self._metadata = metadata
        self._reads = None

    @staticmethod
    def _verify(plates: List[Plate], metadata: ExperimentMetadata) -> None:
        n_plates = len(plates)
        plate_idx = metadata.index.get_level_values('plate')
        n_plates_metadata = plate_idx.max()
        if n_plates != n_plates_metadata:
            warnings.warn('The number of loaded plates (' + str(n_plates) +
                          ') does not match the number of plates in metadata (' +
                          str(n_plates_metadata) + ').', Warning)
        for pid in plate_idx.unique():
            if pid <= n_plates:
                n_wells_metadata = (plate_idx == pid).sum()
                n_wells_plate = plates[pid - 1].samples.size
                if n_wells_metadata > n_wells_plate:
                    warnings.warn('Missing data for ' + str(n_wells_metadata - n_wells_plate) +
                                  ' wells in plate ' + str(pid), Warning)
                elif n_wells_metadata < n_wells_plate:
                    warnings.warn('Missing metadata for ' + str(n_wells_plate - n_wells_metadata) +
                                  ' wells in plate ' + str(pid), Warning)

    @staticmethod
    def _plates_to_df(plates):
        df = None
        for i, plate in enumerate(plates):
            data = plate.data
            idx = data.index.to_frame()
            idx['plate'] = i + 1
            data = data.set_index(pd.MultiIndex.from_frame(idx).reorder_levels(['plate', 'row', 'column']))
            if df is None:
                df = data
            else:
                df = df.append(data)
        return df

    def _process_plates(self, plates: Union[Plate, Iterable[Plate], Iterable[Iterable[Plate]]]) -> List[Plate]:
        if isinstance(plates, Plate):
            return [plates]
        return [CorrectedPlate(p) if isinstance(p, Iterable) else p for p in plates]

    @property
    def metadata(self):
        return self._metadata.copy()

    @property
    def plates(self):
        return self._plates

    @property
    def reads(self):
        if self._reads is None:
            self._reads = self._plates_to_df(self.plates).sort_index()
        return self._reads.copy()

    def add_plates(self, plates: Iterable[Plate], metadata: ExperimentMetadata) -> Experiment:
        plates = self._plates + list(plates)
        metadata = self._metadata.append(metadata)
        self._verify(plates, metadata)
        return self.__class__(plates, metadata)

    def add_plate(self, plate: Plate, metadata: ExperimentMetadata) -> Experiment:
        return self.add_plates([plate], metadata)
