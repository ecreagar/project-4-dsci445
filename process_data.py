import pandas as pd
import numpy as np


def read_data(filename: str):
    return pd.read_csv(filename)


def main():
    filename = "puntreturns.csv"
    data = read_data(filename)


if __name__ == '__main__':
    main()
