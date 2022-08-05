# GDELT
**WARNING**: This repo contains spaghetti code! It is
an experimental, suboptimal, work-in-progress repo.

## What is GDELT?
[Global Database of Events, Language and Tone](https://www.gdeltproject.org/)

## How to Make a GIF
```shell
 convert -delay 30 -loop 0 -resize 750x450  *.png ../animation.gif
```

## Recommendations
+ Use only the GKG raw files to collect article urls & extracted names
+ Use [BoilerPy3](https://github.com/jmriebold/BoilerPy3) 's ArticleExtractor
to get the texts of the articles
+ Put the articles into an ES index and do the term search on that index
+ Your index will be huge, but you'll have a decent db