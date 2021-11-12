from __future__ import annotations

from abc import abstractmethod
from collections import Mapping, Sequence
from datetime import datetime, timedelta
from tecan.util import _str_pp_delta, _split_well
from typing import Iterator, Iterable, Union, Optional, Tuple, Dict, KeysView, ValuesView, ItemsView

import pandas as pd


class GenericMetadata:

    def __init__(self, metadata: Dict[str, object]) -> None:
        self._metadata = metadata

    def __getattr__(self, item) -> object:
        if item in self._metadata.keys():
            return self._metadata[item]
        else:
            raise AttributeError("type object " + type(self).__name__ + " has no attribute '" + str(item) + "'")

    def __repr__(self) -> str:
        return str(self)

    def __str__(self) -> str:
        return type(self).__name__ + '(' + str(self._metadata) + ')'

    def __dict__(self) -> dict:
        return self._metadata.copy()


class InstrumentMetadata(GenericMetadata):
    pass


class PlateMetadata(GenericMetadata):
    pass


class AssayMetadata(GenericMetadata):
    pass


class AbstractAssay:

    def __init__(self, data: pd.DataFrame, metadata: AssayMetadata, plate: Optional[Plate] = None) -> None:
        self._name = data.columns.get_level_values('label').unique()[0]
        self._data = data.droplevel(axis='columns', level='label')
        self._metadata = metadata
        self._plate = plate

    @property
    @abstractmethod
    def data(self) -> pd.DataFrame:
        pass

    @property
    def raw(self) -> pd.DataFrame:
        return self._data

    @property
    def name(self) -> str:
        return self._name

    @property
    def metadata(self) -> AssayMetadata:
        return self._metadata

    def __repr__(self) -> str:
        return str('Assay(' + self.name + '): ' + self.metadata.mode)


class AbstractTimePoint:

    def __init__(self, *args, **kwargs) -> None:
        self._t = timedelta(0)

    @property
    @abstractmethod
    def _t_data(self) -> pd.DataFrame:
        pass

    @property
    def wells(self) -> pd.DataFrame:
        return self._t_data.droplevel([0, 2], axis='columns') \
            .reset_index().pivot(index='row', columns='column').droplevel(0, axis='columns')

    @property
    def data(self) -> pd.DataFrame:
        return pd.DataFrame(self._t_data.values, index=self._t_data.index, columns=['value'])

    @property
    def temperature(self) -> float:
        return self._t_data.columns.get_level_values('temperature')[0]


class SingleAssay(AbstractTimePoint, AbstractAssay):

    def __init__(self, data: pd.DataFrame, metadata: AssayMetadata, plate: Optional[Plate] = None) -> None:
        AbstractAssay.__init__(self, data, metadata, plate)

    @property
    def data(self) -> pd.DataFrame:
        return super().data

    @property
    def _t_data(self) -> pd.DataFrame:
        return self._data


class TimePoint(AbstractTimePoint):

    def __init__(self, ts: TimeSeries, t: timedelta) -> None:
        super().__init__()
        self._ts = ts
        self._t = t

    def __repr__(self) -> str:
        return str('TimeSeries(' + self._ts.name + '): timepoint ' + str(self._t))

    @property
    def _t_data(self) -> pd.DataFrame:
        return self._ts.data.loc[:, pd.IndexSlice[:, self._t]]

    @property
    def time(self) -> timedelta:
        return self._t_data.columns.get_level_values('time')[0]

    @property
    def cycle(self) -> int:
        return int(self._t_data.columns.get_level_values('cycle')[0])


class WellAccessor(Sequence):

    def __init__(self, ts: TimeSeries) -> None:
        self._ts = ts

    def __getitem__(self, k: Union[str, slice, Tuple[slice, slice]]) -> pd.DataFrame:

        if isinstance(k, list):
            ids = [_split_well(i) for i in k]
        elif isinstance(k, tuple) and len(k) == 2:
            ids = pd.IndexSlice[k]
        else:
            ids = pd.IndexSlice[_split_well(k)]

        data = self._ts.data.loc[ids, :]
        if isinstance(data, pd.Series):
            return data.rename('value').to_frame().droplevel(['cycle', 'temperature'])
        else:
            return data.droplevel(['cycle', 'temperature'], axis='columns').T

    def __len__(self) -> int:
        return len(self._ts.data.index)

    def __repr__(self) -> str:
        idx = self._ts.data.index
        wl = list(idx.get_level_values('row').str.cat(idx.get_level_values('column').astype(str))).__repr__()
        return 'WellAccessor(' + wl + ', length=' + str(len(self)) + ')'


class TimeSeries(AbstractAssay, Sequence):

    def __init__(self, data: pd.DataFrame, metadata: AssayMetadata):
        super().__init__(data, metadata)

    def __getitem__(self, t: Union[int, timedelta, datetime]) -> TimePoint:
        if isinstance(t, int):
            t = self.timepoints[t]
        elif isinstance(t, datetime):
            if self._plate and self._plate.timestamp:
                t = t - self._plate.timestamp
            else:
                raise ValueError('plate timestamp is not set')

        return TimePoint(self, t)

    def __len__(self) -> int:
        return len(self.timepoints)

    def __repr__(self) -> str:
        tps = [_str_pp_delta(td) for td in self.timepoints].__repr__()
        return str('TimeSeries(' + tps + ', timepoints=' + str(len(self.timepoints)) +
                   ', wells=' + str(len(self.wells)) + ', mode=\'' + self.metadata.mode + '\')')

    @property
    def data(self) -> pd.DataFrame:
        return self._data.copy()

    @property
    def timepoints(self) -> pd.Index:
        return pd.Index(self.data.columns.get_level_values('time'))

    @property
    def cycles(self) -> pd.DataFrame:
        return pd.DataFrame(self.data.columns.get_level_values('cycle'), index=self.timepoints)

    @property
    def temperatures(self) -> pd.DataFrame:
        return pd.DataFrame(self.data.columns.get_level_values('temperature'), index=self.timepoints)

    @property
    def wells(self) -> WellAccessor:
        return WellAccessor(self)


class AssayCollection(Mapping):

    def __init__(self, assays: Iterable[AbstractAssay]) -> None:
        self._assays = {assay.name: assay for assay in assays}

    def __getitem__(self, k: str) -> AbstractAssay:
        return self._assays[k]

    def __len__(self) -> int:
        return self._assays.values().__len__()

    def __iter__(self) -> Iterator[AbstractAssay]:
        return self._assays.values().__iter__()

    def __repr__(self) -> str:
        return 'AssayCollection(' + list(self._assays.values()).__repr__() + ')'

    def keys(self) -> KeysView[str]:
        return self._assays.keys()

    def values(self) -> ValuesView[AbstractAssay]:
        return self._assays.values()

    def items(self) -> ItemsView[str, AbstractAssay]:
        return self._assays.items()


class Plate:

    def __init__(self, assays: Iterable[AbstractAssay],
                 metadata: PlateMetadata, instrument: InstrumentMetadata) -> None:
        self._assays = AssayCollection(assays)
        self._metadata = metadata
        self._instrument = instrument

    @property
    def data(self) -> pd.DataFrame:
        assay_data = [pd.concat([assay.raw], axis='columns', keys=[assay.name], names=['assay'])
                      for assay in self._assays]
        return pd.concat(assay_data, axis='columns').sort_index(axis='columns')

    @property
    def metadata(self) -> PlateMetadata:
        return self._metadata

    @property
    def assays(self) -> AssayCollection:
        return self._assays

    @property
    def timestamp(self) -> datetime:
        return self._instrument.session_start

    @property
    def instrument(self) -> InstrumentMetadata:
        return self._instrument

    @property
    def start(self) -> datetime:
        return self._metadata.start

    @property
    def end(self) -> datetime:
        return self._metadata.end

    @property
    def samples(self) -> pd.Index:
        return self.data.index

    def __repr__(self) -> str:
        return "Plate(" + str(len(self.assays)) + " assays, " + str(len(self.data.index)) + " samples)"
