import pickle

import pandas as pd
from gdeltdoc import GdeltDoc, Filters

df = pd.read_excel("data/raw/gdelt test data.xlsx")
names = list(df["Name"])

name_data = {}

for name in names:
    name = " ".join(name.lower().split())  # removing extra and non-breaking space
    f = Filters(keyword=name, start_date="2017-01-01", end_date="2021-09-09")
    gd = GdeltDoc()
    articles = gd.article_search(f)
    print(articles)
    name_data[name] = articles

with open("data/interim/gdelt_data.pkl", "wb") as outfile:
    pickle.dump(name_data, outfile)
