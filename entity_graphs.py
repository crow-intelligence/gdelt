import itertools
import random
from datetime import datetime

import networkx as nx
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
article_entities = select([articles.c.locations, articles.c.entities, articles.c.organizations, articles.c.date])
result_porxy = connection.execute(article_entities)
entitiy_results = result_porxy.fetchall()
entity_results = [e for e in entitiy_results if datetime.strptime(e[3][:-6], '%Y%m%d') == datetime(2022, 11, 1)]
entity_results = random.sample(entitiy_results, 1000)
print(f"We have {len(entity_results)} sample articles")

locs = set()
people = set()
orgs = set()

edges = bounter(size_mb=2000)

for res in entitiy_results:
    try:
        locations = res.locations.split(";")
        for loc in locations:
            try:
                locs.add(loc.split("#")[1].split(",")[0])
            except Exception as e:
                continue
    except Exception as e:
        locations = []
    try:
        persons = res.entities.split(";")
        people.update(persons)
    except Exception as e:
        persons = []
    try:
        organizations = res.organizations.split(";")
        orgs.update(organizations)
    except Exception as e:
        organizations = []
    all_entites = locations + persons + organizations
    combos = itertools.combinations(all_entites, 2)
    combos = ("#".join(e) for e in combos)
    edges.update(combos)

print(f"we have {len(locs)} locations, {len(people)}, and {len(orgs)} organizations")
print(f"there are {edges.total()} edges btw those entities")

G = nx.Graph()
for l in locs:
    try:
        G.add_node(l, label=l, type="location")
    except Exception as e:
        continue
for p in people:
    try:
        G.add_node(p, label=p, type="person")
    except Exception as e:
        continue
for o in orgs:
    try:
        G.add_node(o, label=o, type="organization")
    except Exception as e:
        continue

print("The graph has nodes")

for k, v in edges.items():
    if v > 250:
        try:
            w = v / edges.total()
            f, t = k.split("#")
            G.add_edge(f, t, weight=w)
        except Exception as e:
            continue

print("The graph has edges")

nx.write_graphml(G, "data/entity.graphml")
print("The graph has been saved")
print(f"")