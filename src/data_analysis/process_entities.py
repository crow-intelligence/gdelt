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

persons = Table(
    "persons",
    metadata,
    Column("row_id", Integer(), primary_key=True),
    Column("person_name", String(400), nullable=False),
    Column("article_id", Integer(), ForeignKey("articles.article_id")),
    UniqueConstraint("person_name", "article_id"),
)

organizations = Table(
    "organizations",
    metadata,
    Column("row_id", Integer(), primary_key=True),
    Column("organization_name", String(400), nullable=False),
    Column("article_id", Integer(), ForeignKey("articles.article_id")),
    UniqueConstraint("organization_name", "article_id"),
)


metadata.create_all(engine)

################################################################################
#####                         Locations                                    #####
################################################################################
select_locations = select([articles.c.article_id, articles.c.locations])
result_porxy = connection.execute(select_locations)
locations_results = result_porxy.fetchall()

for r in locations_results:
    try:
        loclist = r.locations.split(";")
        article_id = r.article_id
        for location in loclist:
            try:
                country_code = location.split("#")[-5]
                location_name = location.split("#")[1].split(",")[0]
                long = float(location.split("#")[-3])
                lat = float(location.split("#")[-2])
                ins = locations.insert().values(
                    location_name=location_name,
                    article_id=article_id,
                    country_code=country_code,
                    latitude=lat,
                    longitude=long,
                )
                ins.compile().params
                connection.execute(ins)
                print(f"inserted {location_name}")
            except Exception as exc:
                print(f"problem with processing/insert:\t{exc}")
                continue
    except Exception as exc:
        print(f"Exceptin in processing a row:\t{exc}")
        continue

print("Processed location data")

################################################################################
#####                            Persons                                   #####
################################################################################
select_persons = select([articles.c.article_id, articles.c.entities])
result_porxy = connection.execute(select_persons)
persons_results = result_porxy.fetchall()

for r in persons_results:
    try:
        person_list = r.entities.split(";")
        article_id = r.article_id
        for person in person_list:
            try:
                person_name = person.strip()
                ins = persons.insert().values(
                    person_name=person_name,
                    article_id=article_id,
                )
                ins.compile().params
                connection.execute(ins)
                print(f"inserted {person_name}")
            except Exception as exc:
                print(f"problem with processing/insert:\t{exc}")
                continue
    except Exception as exc:
        print(f"Exceptin in processing a row:\t{exc}")
        continue

print("Processed person data")

################################################################################
#####                         Organizations                                #####
################################################################################
select_organizations = select([articles.c.article_id, articles.c.organizations])
result_porxy = connection.execute(select_organizations)
organization_results = result_porxy.fetchall()

for r in organization_results:
    try:
        organization_list = r.organizations.split(";")
        article_id = r.article_id
        for organization in organization_list:
            try:
                organization_name = organization.strip()
                ins = organizations.insert().values(
                    organization_name=organization_name,
                    article_id=article_id,
                )
                ins.compile().params
                connection.execute(ins)
                print(f"inserted {organization_name}")
            except Exception as exc:
                print(f"problem with processing/insert:\t{exc}")
                continue
    except Exception as exc:
        print(f"Exceptin in processing a row:\t{exc}")
        continue

print("Processed organization data")
# TODO: - process data in one round
#       - batch/optimized inserts
