import pandas as pd
import numpy as np


class GenericMetadata:

    def __init__(self, metadata):
        self._metadata = metadata

    def __getattr__(self, item):
        if item in self._metadata.keys():
            return self._metadata[item]
        else:
            raise AttributeError("type object " + type(self).__name__ + " has no attribute '" + str(item) + "'")

    def __repr__(self):
        return str(self)

    def __str__(self):
        return type(self).__name__ + '(' + str(self._metadata) + ')'

    def __dict__(self):
        return self._metadata.copy()


class InstrumentMetadata(GenericMetadata):
    pass


class PlateMetadata(GenericMetadata):
    pass


class AssayMetadata(GenericMetadata):
    pass


class TecanInfinitePlate:

    def __init__(self, data, metadata, instrument):
        self._data = data
        self._metadata = metadata
        self._instrument = instrument

    @property
    def data(self):
        return self._data.copy()

    @property
    def metadata(self):
        return self._metadata

    @property
    def assays(self):
        return self._metadata.assays

    @property
    def timestamp(self):
        return self._instrument.session_start

    @property
    def instrument(self):
        return self._instrument

    @property
    def start(self):
        return self._metadata.start

    @property
    def end(self):
        return self._metadata.end


class TimeLapsePlate(TecanInfinitePlate):
    pass


class SinglePlate(TecanInfinitePlate):

    def __init__(self, data, metadata):
        super().__init__(data, metadata)
        self._data.rename_axis(index='row', columns='column', inplace=True)

    @property
    def data(self):
        return self._data.copy()

    @property
    def metadata(self):
        return self._metadata.copy()

    @property
    def wells(self):
        return self._data.melt(var_name='column', ignore_index=False).set_index('column', append=True).dropna()

    @property
    def timestamp(self):
        return self._metadata.get('timestamp', None)

    @property
    def instrument(self):
        return self._metadata.get('instrument', None)

    @property
    def start(self):
        return self._metadata.get('start', None)

    @property
    def end(self):
        return self._metadata.get('end', None)


class ExperimentMetadata(pd.DataFrame):

    def __init__(self, raw):
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
        super().__init__(raw.set_index(['plate', 'row', 'column']).sort_index())


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
        return self.reads.join(self.metadata, how='outer')

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
