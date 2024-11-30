import pandas as pd 

def extract(file):
    #extract csv
    data = pd.read_csv(file)

    return data.to_dict(orient="records")