#step2:load a pick file with all data structrue
import pickle
import os
def load_from_pickle(path='./data/table.pickle'):
    with open(path,'rb') as f:
        data = pickle.load(f)
    return data


