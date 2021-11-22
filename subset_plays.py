import pandas as pd
from random import sample
data = pd.read_csv('puntreturns.csv')
plays = sample(list(set(data['playId'])), len(set(data['playId']))//100) # subset 1/100th of the data
subset = data.loc[data['playId'].isin(plays)]
subset.to_csv('punts_1percent.csv')
