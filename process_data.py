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
    thindata = data.loc[data['event'].isin(["punt_received", "tackle"])]
    print("Saving file...")
    thindata.to_csv(fileout, index="false")
    print("Done!\n")


def group_data(data: str, fileout: str):
    """
    Group data by gameId and playId.
    """
    print("-----Grouping Data-----")
    print("Loading data...")
    data = pd.read_csv(data)
    print("Grouping data...")
    newdata = data.groupby(["gameId", "playId", "frameId"]).agg({'x': lambda x: ', '.join}).reset_index()
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
