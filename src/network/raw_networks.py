import itertools

import networkx as nx
from bounter import bounter
from sqlalchemy import (
    select,
    MetaData,
    Table,
    Column,
    Integer,
    String,
    ForeignKey,
    UniqueConstraint,
    create_engine,
    Float,
)
from sqlalchemy.orm import sessionmaker


###############################################################################
#####                             Initialize DB                           #####
###############################################################################
# serialize gdelt entity data into a SQLITE db
sqlite_filepath = "gdelt_data.db"
engine = create_engine(f"sqlite:///{sqlite_filepath}")

connection = engine.connect()
Session = sessionmaker(bind=engine)
session = Session()

metadata = MetaData()
locations = Table(
    "locations",
    metadata,
    Column("row_id", Integer(), primary_key=True),
    Column("location_name", String(400), nullable=False, index=True),
    Column("article_id", Integer(), ForeignKey("articles.article_id")),
    Column("country_code", String(5), nullable=False, index=True),
    Column("latitude", Float()),
    Column("longitude", Float()),
    UniqueConstraint("location_name", "article_id"),
)


###############################################################################
#####                             Getting data                            #####
###############################################################################
article_ids = select([locations.c.article_id])
article_porxy = connection.execute(article_ids)
article_results = article_porxy.fetchall()
article_results = set([e[0] for e in article_results])
print("We have article ids")

location_edges = bounter(size_mb=2048)
print("Let's fill the bounter!")
i = 0
for article_id in article_results:
    try:
        res = (
            session.query(locations.c.location_name)
            .filter(locations.c.article_id == article_id)
            .all()
        )
        res = sorted([e[0] for e in res])
        edges = itertools.combinations(res, 2)
        for edge in edges:
            edge = "#".join(edge)
            location_edges.update([edge])
            i += 1
            print(i)
    except Exception as e:
        print(f"problem with making edges:\t{e}")
print("The bounter has been filled")


###############################################################################
#####                        Making a raw graph                           #####
###############################################################################
G = nx.Graph()

for k, v in location_edges.items():
    try:
        e1, e2 = k.split("#")
        G.add_edge(e1, e2, weight=v)
    except Exception as e:
        print(f"probelm with getting edge:\{e}")
        continue
print("The graph is ready to be serialized")
nx.write_graphml(G, "data/interim/raw_location.graphml")
print(f"Serialized GRAPH with\nNODES:\t{len(G.nodes)}\nEDGES:\t{len(G.edges)}")
print("The GRAPH is READY!" * 10)
