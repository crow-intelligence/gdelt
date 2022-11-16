import asyncio
import random
from datetime import datetime

import aiohttp
import pandas as pd
from boilerpy3 import extractors
from sqlalchemy import (
    select,
    MetaData,
    Table,
    Column,
    Integer,
    String,
    create_engine,
)


###############################################################################
#####                             Initialize DB                           #####
###############################################################################
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


########################################################################################################################
#####                                          Getting urls                                                        #####
########################################################################################################################
article_urls = select([articles.c.article_id, articles.c.article_url, articles.c.date])
result_porxy = connection.execute(article_urls)
url_results = result_porxy.fetchall()
url_results = [e for e in url_results if datetime.strptime(e[2][:-6], '%Y%m%d') == datetime(2022, 11, 1)]
url_results = random.sample(url_results, 10000)
print(f"We have {len(url_results)} sample articles")

ids = []
urls = []
texts = []


async def main():
    async with aiohttp.ClientSession() as session:
        for e in url_results:
            url = e[1]
            try:
                async with session.get(url) as resp:
                    # TODO: use playwright/selenium for prerendering
                    # TODO: rotate IP to avoid banning
                    html = await resp.text()
                    extractor = extractors.ArticleExtractor()

                    text = extractor.get_content(html).replace("\t", " ").replace("\n", " ")
                    text = text.split()
                    text = [e.strip() for e in text]
                    text = " ".join(text)

                    ids.append(e[0])
                    urls.append(e[1])
                    texts.append(text)
            except Exception as e:
                print(e)
                continue

print("Start getting articles")
asyncio.run(main())
assert len(ids) == len(urls) == len(texts)
print("Url harvest has just finished")

data_dict = {"id": ids,
             "urls": urls,
             "text": texts}

df = pd.DataFrame(data_dict)
df.to_csv("data/articles.tsv", index=False, sep="\t")
print("tsv filed saved, well done!")
