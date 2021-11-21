from abc import abstractmethod
from datetime import timedelta, datetime
from typing import Sequence, Union, Optional, Tuple

import pandas as pd

from base.util import strffdelta, strpdelta, strpwell


class Accessor:

    @staticmethod
    def _idx(df, k, f):
        n = df.columns.names
        ix = n.index(f)
        return tuple(k if i == ix else slice(None) for i in range(len(n)))


class AssayAccessor(Accessor, Sequence):

    def __getitem__(self, k: Union[int, str]) -> pd.DataFrame:
        if isinstance(k, int):
            k = self._assay_idx[k]
        return self._assay_df.loc[:, self._idx(self._assay_df, k, 'assay')] \
                   .droplevel([i for i in self._assay_df.columns.names if i not in ['time']], axis='columns')

    def __len__(self) -> int:
        return len(self._assay_idx)

    def __repr__(self) -> str:
        return self.__class__.__name__ + '(' + list(self._assay_idx).__repr__() + ')'

    @property
    def _assay_idx(self) -> pd.Index:
        return self._assay_df.columns.get_level_values('assay').unique()

    @property
    @abstractmethod
    def _assay_df(self) -> pd.DataFrame:
        pass


class TimeAccessor(Accessor, Sequence):

    def __getitem__(self, k: Union[int, timedelta, datetime]) -> pd.DataFrame:

        def ctd(t: Union[slice, str, int, datetime, timedelta]) -> Union[slice, timedelta]:
            if isinstance(t, slice):
                return slice(*[ctd(i) for i in (t.start, t.stop, t.step)])
            elif isinstance(t, str):
                return strpdelta(t)
            elif isinstance(t, int):
                return self._time_idx[t]
            elif isinstance(t, datetime):
                if self._time_ref:
                    return t - self._time_ref
                else:
                    raise ValueError('unable to determine time reference')
            else:
                return t

        return self._time_df.loc[:, self._idx(self._time_df, ctd(k), 'time')] \
                   .droplevel([i for i in self._time_df.columns.names if i not in ['assay', 'time']], axis='columns')

    def __len__(self) -> int:
        return len(self._time_idx)

    def __repr__(self) -> str:
        return self.__class__.__name__ + '(' + [strffdelta(td) for td in self._time_idx].__repr__() + ')'

    @property
    def _time_idx(self) -> pd.Index:
        return self._time_df.columns.get_level_values('time').unique()

    @property
    def _time_ref(self) -> Optional[datetime]:
        return None

    @property
    @abstractmethod
    def _time_df(self) -> pd.DataFrame:
        pass


class WellAccessor(Accessor, Sequence):

    def __getitem__(self, k: Union[str, slice, Tuple[slice, slice]]) -> pd.DataFrame:
        if isinstance(k, list):
            idx = [self._idx(self._well_df, strpwell(i)) for i in k]
        else:
            idx = self._idx(self._well_df, k if (isinstance(k, tuple) and len(k) == 2) else strpwell(k))

        return self._well_df.loc[idx, :]\
                   .droplevel([i for i in self._well_df.columns.names if i not in ['assay', 'time']], axis='columns')

    def __len__(self) -> int:
        return len(self._well_idx)

    def __repr__(self) -> str:
        wl = list(self._well_idx.get_level_values('row').str
                      .cat(self._well_idx.get_level_values('column').astype(str))).__repr__()
        return self.__class__.__name__ + '(' + wl + ')'

    @staticmethod
    def _idx(df, k, f=None):
        n = df.index.names
        rix = n.index('row')
        cix = n.index('column')
        row, column = k
        return tuple(row if i == rix else column if i == cix else slice(None) for i in range(len(n)))

    @property
    def _well_idx(self) -> pd.Index:
        return self._well_df.index

    @property
    @abstractmethod
    def _well_df(self) -> pd.DataFrame:
        pass
