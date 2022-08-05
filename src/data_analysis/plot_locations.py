import altair as alt
import pandas as pd
from vega_datasets import data

# alt.data_transformers.disable_max_rows()

df = pd.read_csv("data/processed/locationdata.tsv", sep="\t")
dates = set(df["Date"])


for date in dates:
    date_df = df[df["Date"] == date]
    mentions_sum = sum(date_df["Mentions"])
    proportions = [(e / mentions_sum) * 100 for e in date_df["Mentions"]]
    date_df["Proportion"] = proportions
    date_df = date_df[date_df["Proportion"].between(0.01, 7)]
    countries = alt.topo_feature(data.world_110m.url, "countries")
    date_df["Proportion"] = date_df["Proportion"] * 10000
    colors = ["brown"] * len(date_df["Proportion"])
    date_df["Color"] = colors
    print(date_df.size)

    background = (
        alt.Chart(countries)
        .mark_geoshape(fill="lightgray", stroke="white")
        .project("equirectangular")
        .properties(width=1500, height=900, title=date)
    )
    background.configure_title(
        fontSize=30, font="Courier", anchor="start", color="black"
    )
    points = (
        alt.Chart(date_df)
        .mark_circle(color="brown", opacity=0.55, size=10)
        .encode(longitude="Long:Q", latitude="Lat:Q", size="Proportion:Q")
        .properties(title=date)
    )
    points.configure_title(fontSize=30, font="Courier", anchor="start", color="black")
    chart = background + points
    chart.save(f"vizs/timelapse/png/{date}.png")
    print(f"Saved plot for {date} as an png")

print("Hey, we've done!" * 100)
