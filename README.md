# pyLucyReader
## Read and graph bioluminescence assays captured with Tecan plate readers

### Reading files:
```python3
from luciferase.io import DualLuciferaseExperimentReader

experiment = DualLuciferaseExperimentReader.read(
    'metadata.xlsx',
    [
        'plate_1_firefly.xlsx',
        'plate_2_firefly.xlsx',
        [
            'plate_3_firefly.xlsx',
            'plate_3_firefly_redo.xlsx'
        ]
    ], [
        'plate_1_renilla.xlsx',
        'plate_2_renilla.xlsx',
        'plate_3_renilla.xlsx'
    ]
)
```

### Plotting results:
```python3
from matplotlib import pyplot as plt
from luciferase import graphs
import pandas as pd

idx = pd.IndexSlice
n = experiment.normalize(background='neg').sort_index()
d = n.set_index(['induction', 'sample', 'plate']) \
     .loc[:, ['normalized']] \
     .dropna() \
     .sort_index()

fig, ax = plt.subplots()
ax = graphs.catplot(d, ax=ax, spacing=0.5,
    title='Luciferase assay for the MT promoter',
    ylabel='Normalized signal (ff/rn) [au]',
    xlabel='CuSO$_4$ concentration [μm]')
```
