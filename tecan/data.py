from __future__ import annotations

from abc import abstractmethod
from collections import Mapping, Sequence
from datetime import datetime, timedelta
from base.util import strffdelta, strpwell
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

    def __iter__(self) -> Iterator:
        return self._metadata.copy().items().__iter__()


class InstrumentMetadata(GenericMetadata):
    pass


class PlateMetadata(GenericMetadata):
    pass


class AssayMetadata(GenericMetadata):
    pass


class AbstractAssay:

    def __init__(self, data: pd.DataFrame, metadata: AssayMetadata, plate: Optional[Plate] = None) -> None:
        self._name = data.columns.get_level_values('label').unique()[0]
        self._data = data.droplevel(axis='columns', level=['cycle', 'label', 'temperature'])
        idx = data.columns.get_level_values('time')
        self._cycles = pd.Series(data.columns.get_level_values('cycle'), idx)
        self._temperatures = pd.Series(data.columns.get_level_values('temperature'), idx)
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
        return str('Assay(name=' + self.name + ', mode=' + self.metadata.mode + ') ')


class AbstractTimePoint:

    def __init__(self, *args, **kwargs) -> None:
        self._t = timedelta(0)

    @property
    @abstractmethod
    def data(self) -> pd.DataFrame:
        pass

    @property
    @abstractmethod
    def temperature(self) -> float:
        pass

    @property
    @abstractmethod
    def cycle(self) -> float:
        pass

    @property
    def time(self):
        return self._t

    @property
    def wells(self) -> pd.DataFrame:
        return self.data.reset_index().pivot(index='row', columns='column').droplevel(0, axis='columns')


class SingleAssay(AbstractTimePoint, AbstractAssay):

    def __init__(self, data: pd.DataFrame, metadata: AssayMetadata, plate: Optional[Plate] = None) -> None:
        AbstractAssay.__init__(self, data, metadata, plate)

    @property
    def data(self) -> pd.DataFrame:
        return self._data

    @property
    def temperature(self) -> float:
        return self._temperatures.iloc[0]

    @property
    def cycle(self) -> float:
        return self._cycles.iloc[0]


class TimePoint(AbstractTimePoint):

    def __init__(self, ts: TimeSeries, t: timedelta) -> None:
        super().__init__()
        self._ts = ts
        self._t = t

    def __repr__(self) -> str:
        return str('TimeSeries(' + self._ts.name + '): timepoint ' + str(self._t))

    @property
    def data(self) -> pd.DataFrame:
        return self._ts.data.loc[:, self._t]

    @property
    def temperature(self) -> float:
        return self._ts.temperatures.loc[self._t]

    @property
    def cycle(self) -> float:
        return self._ts.cycles.loc[self._t]


class AbstractTimeSlice:

    @property
    @abstractmethod
    def timepoints(self) -> pd.Index:
        pass

    @property
    @abstractmethod
    def cycles(self) -> pd.DataFrame:
        pass

    @property
    @abstractmethod
    def temperatures(self) -> pd.DataFrame:
        pass


class TimeAccessor(AbstractTimeSlice):

    def __init__(self, ts: TimeSeries, s: slice) -> None:
        super().__init__()
        self._ts = ts
        data = self._ts.data
        start, stop, step = s.start, s.stop, s.step
        if s.start is not None and not isinstance(s.start, timedelta):
            start = data.columns.get_level_values('time')[s.start]
        if s.stop is not None and not isinstance(s.stop, timedelta):
            stop = data.columns.get_level_values('time')[s.stop]
        if s.step is not None and not isinstance(s.step, timedelta):
            step = data.columns.get_level_values('time')[s.step]
        self._slice = slice(start, stop, step)

    def __repr__(self) -> str:
        return str('TimeAccessor(' + self._ts.name + ', slice=' + str(self._slice) + ')')

    @property
    def data(self):
        return self._ts.data.loc[:, self._slice]

    @property
    def timepoints(self) -> pd.Index:
        return self._ts.timepoints[self._slice]

    @property
    def cycles(self) -> pd.DataFrame:
        return self._ts.cycles[self._slice]

    @property
    def temperatures(self) -> pd.DataFrame:
        return self._ts.temperatures[self._slice]


class WellAccessor(Sequence):

    def __init__(self, ts: TimeSeries) -> None:
        self._ts = ts

    def __getitem__(self, k: Union[str, slice, Tuple[slice, slice]]) -> pd.DataFrame:

        if isinstance(k, list):
            ids = [strpwell(i) for i in k]
        elif isinstance(k, tuple) and len(k) == 2:
            ids = pd.IndexSlice[k]
        else:
            ids = pd.IndexSlice[strpwell(k)]

        data = self._ts.data.loc[ids, :]
        if isinstance(data, pd.Series):
            return data.rename('value').to_frame()
        else:
            return data.T

    def __len__(self) -> int:
        return len(self._ts.data.index)

    def __repr__(self) -> str:
        idx = self._ts.data.index
        wl = list(idx.get_level_values('row').str.cat(idx.get_level_values('column').astype(str))).__repr__()
        return 'WellAccessor(' + wl + ', length=' + str(len(self)) + ')'


class TimeSeries(AbstractAssay, Sequence, AbstractTimeSlice):

    def __init__(self, data: pd.DataFrame, metadata: AssayMetadata):
        super().__init__(data, metadata)

    def __getitem__(self, t: Union[int, timedelta, datetime]) -> Union[TimePoint, TimeAccessor]:
        if isinstance(t, int):
            t = self.timepoints[t]
        elif isinstance(t, datetime):
            if self._plate and self._plate.timestamp:
                t = t - self._plate.timestamp
            else:
                raise ValueError('plate timestamp is not set')
        elif isinstance(t, slice):
            return TimeAccessor(self, t)

        return TimePoint(self, t)

    def __len__(self) -> int:
        return len(self.timepoints)

    def __repr__(self) -> str:
        tps = [strffdelta(td) for td in self.timepoints].__repr__()
        return str('TimeSeries(' + tps + ', timepoints=' + str(len(self.timepoints)) +
                   ', wells=' + str(len(self.wells)) + ', mode=\'' + self.metadata.mode + '\')')

    @property
    def data(self) -> pd.DataFrame:
        return self._data.copy()

    @property
    def wells(self) -> WellAccessor:
        return WellAccessor(self)

    @property
    def timepoints(self) -> pd.Index:
        return self._data.columns.get_level_values('time')

    @property
    def cycles(self) -> pd.DataFrame:
        return pd.DataFrame(self._cycles, index=self.timepoints)

    @property
    def temperatures(self) -> pd.DataFrame:
        return pd.DataFrame(self._temperatures, index=self.timepoints)


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
