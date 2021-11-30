import pandas as pd
import numpy as np
import os


def merge_stats(punts: str, scoutfile: str, fileout: str):
    """
    Join punt returns and scout data in order to gain extra statistics.
    """
    print("-----Merging Datasets-----")
    print("Loading data...")
    punts = pd.read_csv(punts)
    scoutdata = pd.read_csv(scoutfile, usecols=["gameId", "playId", "hangTime"])
    print("Performing merge...")
    merged = punts.set_index(['gameId', 'playId']).join(scoutdata.set_index(['gameId', 'playId']), how='inner')
    print("Creating output file...")
    merged.to_csv(fileout, index="false")
    print("Done!\n")
    return merged


def remove_features(data: str, fileout: str):
    """
    Remove all features we won't be using.
    """
    print("-----Removing Unnecessary Features-----")
    print("Loading data...")
    data = pd.read_csv("mergeStats.csv")
    print("Subsetting data...")
    # Columns to be kept
    thindata = data[["gameId", "playId", "specialTeamsResult", "x", "y", "dis", "event", "nflId", "team",
                     "frameId", "playDirection", "displayName", "hangTime"]]
    # Events to be kept
    thindata = data.loc[(data['event'].isin(["punt", "punt_received", "tackle"])) & ~(data['specialTeamsResult'].isin(['Fair Catch']))]
    print("Saving file...")
    thindata.to_csv(fileout, index="false")
    print("Done!\n")


def get_punt_information(data: pd.DataFrame):

    data = data[['gameId','playId','frameId','specialTeamsResult','x','y',
                 'event','nflId','displayName','position','team','hangTime']]
    data.loc[:,'puntReturner'] = ""
    newdata = []

    aggregations = {'specialTeamsResult': 'first',
                'x': lambda x: list(x),
                'y': lambda x: list(x),
                'event': 'first',   
                'nflId': lambda x: list(x),
                'displayName': lambda x: list(x),   
                'position': lambda x: list(x),  
                'team': lambda x: list(x),
                'hangTime': 'first'}

    returns = data.groupby(['gameId','playId',
                            'frameId']).aggregate(aggregations).groupby(['gameId',
                                                                         'playId']).aggregate(lambda x: 
                                                                                              list(x))

    for idx,row in returns.reset_index().iterrows():
        if len(row['event']) < 3:
            continue
        newrow = {}
        awayplayers = []
        homeplayers = []
        
        # Get the overall information
        newrow['gameId'] = row['gameId']
        newrow['playId'] = row['playId']
        newrow['specialTeamsResult'] = row['specialTeamsResult'][0]
        newrow['hangTime'] = row['hangTime'][0]
        
        # Get the information about the punt
        ballidx_punt = [x for x in range(len(row['position'][0])) if row['position'][0][x] != row['position'][0][x]][0]
        punt_position_x = row['x'][0][ballidx_punt]
        punt_position_y = row['y'][0][ballidx_punt]
        punteridx_punt = [x for x in range(len(row['position'][0])) if (row['position'][0][x] == "P") or (row['position'][0][x] == "K")][0]
        punting_team = row['team'][0][punteridx_punt]
        newrow['punt_position_x'] = punt_position_x
        newrow['punt_position_y'] = punt_position_y
        
        # Get the information about the reception
        for idx,team in enumerate(row['team'][1]):
            if team == 'home':
                homeplayers.append((row['x'][1][idx], row['y'][1][idx]))
            if team == 'away':
                awayplayers.append((row['x'][1][idx], row['y'][1][idx]))
        
        for i in range(1,12):
            newrow[f'away_player_{i}'] = awayplayers[i-1]
            newrow[f'home_player_{i}'] = homeplayers[i-1]

        ballidx_field = [x for x in range(len(row['position'][1])) if row['position'][1][x] != row['position'][1][x]][0]
        field_position_x = row['x'][1][ballidx_punt]
        field_position_y = row['y'][1][ballidx_punt]
        newrow['field_position_x'] = field_position_x
        newrow['field_position_y'] = field_position_y

        # Get the information about the reception
        ballidx_tackle = [x for x in range(len(row['position'][2])) if row['position'][2][x] != row['position'][2][x]][0]
        tackle_position_x = row['x'][2][ballidx_tackle]
        tackle_position_y = row['y'][2][ballidx_tackle]
        newrow['tackle_position_x'] = tackle_position_x
        newrow['tackle_position_y'] = tackle_position_y
        
        newdata.append(newrow)
    
    puntinfo = pd.DataFrame(newdata)
    puntinfo['yards_gained'] = puntinfo.apply(lambda x: (x['tackle_position_x'] - x['field_position_x']) if \
                                                         x['playDirection'] == 'left' else \
                                                        (x['field_position_x'] - x['tackle_position_x']), axis=1)
    return puntinfo

def group_data(data: str, fileout: str):
    """
    Group data by gameId and playId.
    """
    print("-----Grouping Data-----")
    print("Loading data...")
    data = pd.read_csv(data)
    print("Grouping data...")
    newdata = get_punt_information(data)
    print("Saving file...")
    newdata.to_csv(fileout, index="false")
    # print(newdata)
    print("Done!\n")


def main():
    filename = "puntreturns.csv"
    merge_stats(filename, scoutfile="Data/PFFScoutingData.csv", fileout="mergeStats.csv")
    remove_features(data="mergeStats.csv", fileout="removeFeatures.csv")
    group_data(data="removeFeatures.csv", fileout="groupedData.csv")
    # os.remove("mergeStats.csv")
    # os.remove("removeFeatures.csv")


if __name__ == '__main__':
    main()
