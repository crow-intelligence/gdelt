import pickle

name_data = pickle.load(open("data/interim/gdelt_data.pkl", "rb"))

with open("data/processed/test_data.tsv", "w") as outfile:
    for name in name_data:
        df = name_data[name]
        if not df.columns.empty:
            urls = list(df["url"])
            titles = list(df["title"])
            langs = list(df["language"])
            countrycodes = list(df["sourcecountry"])
            n_names = [name] * len(urls)
            assert (
                len(urls)
                == len(titles)
                == len(langs)
                == len(countrycodes)
                == len(n_names)
            )
            lines = zip(n_names, urls, titles, langs, countrycodes)
            for l in lines:
                o = "\t".join(l) + "\n"
                outfile.write(o)
        else:
            o = name + "\tNone\tNone\tNone\tNone\n"
            outfile.write(o)
