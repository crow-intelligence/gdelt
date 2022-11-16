import io
import zipfile
from datetime import date, datetime, timedelta

import pandas as pd
import requests
from gdeltdoc import GdeltDoc, Filters
from sqlalchemy import (
    insert,
    select,
    desc,
    func,
    cast,
    and_,
    or_,
    not_,
    update,
    delete,
    text,
    MetaData,
    Table,
    Column,
    Integer,
    Numeric,
    String,
    DateTime,
    ForeignKey,
    UniqueConstraint,
    create_engine,
)

# TODO: clean up imports!
###############################################################################
#####                             Settings                                #####
###############################################################################
# set the search range here
start_day = "2022-09-01"  # set the starting date here, test only with one or two days!
today = str(date.today())
# get a tuple of dates which can be supplied to str.starts with to filter out urls
date_range = pd.date_range(
    datetime.strptime(start_day, "%Y-%m-%d"),
    datetime.strptime(today, "%Y-%m-%d") - timedelta(days=1),
    freq="d",
)
date_range = list(date_range)
date_range = tuple([str(e).split()[0].replace("-", "") for e in date_range])

###############################################################################
#####                             Initialize DB                           #####
###############################################################################
# masterfile updated at every 15 mins
resp = requests.get("http://data.gdeltproject.org/gdeltv2/masterfilelist.txt").text

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
#####                    Fill articles table                              #####
###############################################################################
# TODO: you can use async with other dbs, SQLite3 doesn't allow it
# make sure that you collect only unseen data
select_source_urls = select([articles.c.source_url])
result_porxy = connection.execute(select_source_urls)
results = result_porxy.fetchall()
sources_collected = set([e[0] for e in results])
print(f"We've already collected {len(sources_collected)} items")

i = 0
for l in resp.split("\n"):
    try:
        _, _, url = l.split()
        urldate = url.split("/")[-1].split(".")[0]
        # the header file for the gkg tstv https://github.com/linwoodc3/gdelt2HeaderRows/blob/master/schema_csvs/GDELT_2.0_gdeltKnowledgeGraph_Column_Labels_Header_Row_Sep2016.tsv
        if (
            urldate.startswith(date_range)
            and "gkg" in url
            and url not in sources_collected
        ):
            # unzipp and read in memory, the unzipped files are about 20MB
            r = requests.get(url)
            z = zipfile.ZipFile(io.BytesIO(r.content))
            t = z.read(z.filelist[0].filename)
            try:
                df = pd.read_csv(io.StringIO(t.decode("utf-8")), sep="\t", header=None)
            except Exception as e:
                print(f"problem with making a df:\t{e}")
                continue
            urls = df.iloc[:, 4]
            # locations
            location_entities = df.iloc[:, 9]
            # persons
            person_entities = df.iloc[:, 11]
            # organizations
            org_entities = df.iloc[:, 13]
            # themes
            themes = df.iloc[:, 7]
            # tones
            tones = df.iloc[:, 15]
            article2information = zip(
                urls, location_entities, person_entities, org_entities, themes, tones
            )
            for information in article2information:
                try:
                    ins = articles.insert().values(
                        date=urldate,
                        source_url=url,
                        article_url=information[0],
                        locations=information[1],
                        entities=information[2],
                        organizations=information[3],
                        themes=information[4],
                        tones=information[5],
                    )
                    ins.compile().params
                    connection.execute(ins)
                except Exception as e:
                    print(f"data insertion failed:\t{e}")
                    continue
        i += 1
        print(f"processed {i} items")
    except Exception as e:
        print(f"getting data failed with:\t{e}")
        continue
print("WE'VE GOT THE RAW DATA!!!!!!!!!\n" * 100)
