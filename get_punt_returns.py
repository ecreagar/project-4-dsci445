import pandas as pd
import numpy as np
import sys,os

def read_data(filename: str):
	return pd.read_csv(filename)


def punt_return_ids(data: pd.DataFrame):
	"""
	Select the punts that were returned from the plays.csv file (about 2000 total)
	"""
	acceptable = ['Return', 'Fair Catch']
	return data.loc[(data['specialTeamsPlayType'] == 'Punt') & 
					(data['specialTeamsResult'].isin(acceptable))][['gameId','playId','specialTeamsResult']]


def extract_punts_oneyear(filename: str, 
						  ids: pd.DataFrame, 
						  fileout: str):
	"""
	filename:
		the tracking data for one year (2018, 2019, or 2020)
	ids:
		the punt return Ids generated from the punt_return_ids function.
	fileout:
		the file name to save
	"""
	tracking_data = read_data(filename)
	# inner join the data so we only retain the punts.
	punts = ids.set_index(['gameId',
			       'playId']).join(tracking_data.set_index(['gameId',
						   	 		'playId']), how='inner')
	punts.to_csv(fileout)


def combine_punts(punt_files: list,
				  fileout: str,
				  delete: bool=True):
	data = pd.DataFrame()
	for i in punt_files:
		data = data.append(read_data(i))
		if delete:
			os.remove(i) # delete the one year punt files as we go.
	data.to_csv(fileout)



def main():
	filename = 'Data/plays.csv'
	data = read_data(filename)
	ids = punt_return_ids(data)
	for i in [2018, 2019, 2020]:
		extract_punts_oneyear(filename=f'tracking{i}.csv', 
							  ids=ids, 
							  fileout=f'tracking{i}_punts.csv')
	combine_punts(['tracking2018_punts.csv',
				   'tracking2019_punts.csv',
				   'tracking2020_punts.csv'],
				  fileout='puntreturns.csv',
				  delete=True)




if __name__ == '__main__':
	main()
