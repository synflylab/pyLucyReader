import pandas as pd
import numpy as np


class LuciferaseExperiment:

    def __init__(self, metadata, plates):
        self._metadata = metadata
        self._plates = plates
        self._reads = None

    @property
    def metadata(self):
        return self._metadata.copy()

    @property
    def plates(self):
        return self._plates

    @property
    def reads(self):
        if self._reads is None:
            self._reads = self._plates_to_df(self.plates)
        return self._reads.copy()

    @staticmethod
    def _plates_to_df(plates):
        df = None
        for i, plate in enumerate(plates):
            data = plate.wells
            data['date'] = plate.timestamp.date()
            data['plate'] = i + 1
            data = data.set_index('plate', append=True).reorder_levels(['plate', 'row', 'column'])
            if df is None:
                df = data
            else:
                df = df.append(data)
        return df


class DualLuciferaseExperiment(LuciferaseExperiment):

    def __init__(self, metadata, firefly, renilla):
        super().__init__(metadata, firefly)
        self._norms = renilla

    @property
    def norms(self):
        return self._norms

    @property
    def firefly(self):
        return self.reads[['firefly']]

    @property
    def firefly_plates(self):
        return self._plates

    @property
    def renilla(self):
        return self.reads[['renilla']]

    @property
    def renilla_plates(self):
        return self._norms

    @property
    def reads(self):
        if self._reads is None:
            firefly = self._plates_to_df(self.firefly_plates).rename({'value': 'firefly'}, axis='columns')
            renilla = self._plates_to_df(self.renilla_plates).rename({'value': 'renilla'}, axis='columns')
            self._reads = firefly.join(
                renilla.drop('date', axis='columns'))[['date', 'firefly', 'renilla']].sort_index()
        return self._reads.copy()

    @property
    def wells(self):
        return self.reads.join(self.metadata, how='outer', rsuffix='_meta')

    def normalize(self, background='null', to=None, on=None, threshold=0):
        raw = self.wells
        raw['value'] = raw['firefly'] / raw['renilla']

        idx = pd.IndexSlice
        controls = raw.loc[raw['sample'] == background, :].groupby(['plate'])
        thresholds = 2 * (controls.mean()['renilla'] + controls.std()['renilla']) + threshold
        for plate in thresholds.index.unique('plate'):
            raw.loc[idx[plate, raw['renilla'] < thresholds[plate]], 'value'] = np.nan
        raw['normalized'] = raw['value']

        if to is not None and on is not None:
            ref = raw.loc[to] \
                     .rename({'value': 'reference'}, axis='columns') \
                     .groupby(on) \
                     .mean()[['reference']]
            raw = raw.join(ref, on=on)
            raw['normalized'] = raw['value'] / raw['reference']
            raw = raw.drop('reference', axis='columns')

        return raw.sort_index()
