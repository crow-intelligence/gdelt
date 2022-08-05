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

###############################################################################
#####                             Settings                                #####
###############################################################################
# set the search range here
start_day = "2022-02-24"  # set the starting date here, test only with one or two days!
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
    Column("entities", String(9999), nullable=False),
)

entities = Table(
    "entities",
    metadata,
    Column("row_id", Integer(), primary_key=True),
    Column("entity_name", String(400), nullable=False),
    Column("article_id", Integer(), ForeignKey("articles.article_id")),
    Column("article_url", String(2000)),
    Column("search_term", String(255), nullable=False),
    UniqueConstraint("entity_name", "article_id", name="entity2article"),
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
                try:
                    df = pd.read_csv(
                        io.StringIO(t.decode("latin-1")), sep="\t", header=None
                    )
                except Exception as e:
                    print(f"another problem with the df:\t{e}")
                    continue
            urls = df.iloc[:, 4]
            a_entities = df.iloc[:, 11]
            article2entity = zip(urls, a_entities)
            for a2e in article2entity:
                try:
                    ins = articles.insert().values(
                        date=urldate,
                        source_url=url,
                        article_url=a2e[0],
                        entities=a2e[1],
                    )
                    ins.compile().params
                    connection.execute(ins)
                except Exception as e:
                    print(f"data insertion failed:\t{e}")
                    continue
    except Exception as e:
        print(f"getting data failed with:\t{e}")
        continue
print("WE'VE GOT THE RAW DATA!!!!!!!!!\n" * 100)
###############################################################################
#####                  Get articles containing search terms               #####
###############################################################################
search_terms = ["dual use", "money laundering"]
term_data = {}
# get articles for each term
for term in search_terms:
    term = " ".join(term.lower().split())  # removing extra and non-breaking space
    f = Filters(keyword=term, start_date=start_day, end_date=today)
    gd = GdeltDoc()
    news_articles = gd.article_search(f)
    term_data[term] = news_articles
print("WE'VE GOT TERM DATA!!!!!!!!!\n" * 100)
###############################################################################
#####                        Fill entities table                          #####
###############################################################################
for k, v in term_data.items():
    term_urls = list(term_data[k]["url"])
    for url in term_urls:
        s = select([articles]).where(articles.c.article_url == url)
        rp = connection.execute(s)
        record = rp.first()
        if record:
            print(record)
            article_id = record[0]
            article_date = record[1]
            source_url = record[2]
            article_url = record[3]
            article_entities = record[4].split(";")
            for article_entity in article_entities:
                try:
                    ins = entities.insert().values(
                        entity_name=article_entity,
                        article_id=article_id,
                        article_url=article_url,
                        search_term=k,
                    )
                    ins.compile().params
                    connection.execute(ins)
                except Exception as e:
                    print(f"data insertion failed:\t{e}")
                    continue
print("THE ENTITIES TABLE IS READY!!!!!!!!!\n" * 100)
