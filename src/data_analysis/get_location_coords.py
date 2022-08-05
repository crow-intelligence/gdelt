import io
from datetime import datetime

import pandas as pd
from bounter import bounter
from sqlalchemy import (
    select,
    MetaData,
    Table,
    Column,
    Integer,
    String,
    create_engine,
)

################################################################################
#####                        Connect to db                                 #####
################################################################################
# serialize gdelt entity data into a SQLITE db
sqlite_filepath = "gdelt_data.db"
engine = create_engine(f"sqlite:///{sqlite_filepath}")
connection = engine.connect()

metadata = MetaData()

# create tables
articles = Table(
    "articles",
    metadata,
    Column("article_id", Integer(), primary_key=True),
    Column("date", String(50), nullable=False),
    Column("source_url", String(2000), nullable=False),
    Column("article_url", String(2000), nullable=False, unique=True),
    Column(
        "locations",
        String(9999),
    ),
    Column(
        "entities",
        String(9999),
    ),
    Column(
        "organizations",
        String(9999),
    ),
    Column(
        "themes",
        String(9999),
    ),
    Column(
        "tones",
        String(9999),
    ),
)


metadata.create_all(engine)
# TODO: switch off verbose mode in production
###############################################################################
#####                          Get lang-lot coords                        #####
###############################################################################
select_source_urls = select([articles.c.locations, articles.c.date])
result_porxy = connection.execute(select_source_urls)
results = result_porxy.fetchall()

latlongs = bounter(size_mb=3072)


for r in results:
    try:
        loclist = r.locations.split(";")
        ddata = r.date
        for loc in loclist:
            try:
                lat = loc.split("#")[-3]
                long = loc.split("#")[-2]
                tstamped_ll = "#".join([ddata[:8], lat, long])
                latlongs.update([tstamped_ll])
            except Exception as e2:
                print(f"inside:\t{e2} because of\t{r}")
    except Exception as e1:
        print(f"outside:\t{e1} because of\t{r}")
        continue


h = "Date\tLat\tLong\tMentions\n"
lines = []
for k, v in latlongs.items():
    try:
        k = k.split("#")
        ddate = datetime.strptime(k[0], "%Y%m%d").strftime("%Y-%m-%d")
        o = ddate + "\t" + k[1] + "\t" + k[2] + "\t" + str(v) + "\n"
        lines.append(o)
    except Exception as e:
        print(e)
        continue

lines = [h] + lines
lines = io.StringIO("".join(lines))
df = pd.read_csv(lines, sep="\t")
df = df.sort_values(by="Date")
df.to_csv("data/processed/locationdata.tsv", index=False, sep="\t")
print("location data is ready")
