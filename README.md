# WE1S Tweet Suite

The WE1S Tweet Suite is a suite of tools for collecting and preprocessing data from Twitter, and for integrating that data into the WE1S Jupyter notebook workspace.

## Scraper

The Scraper tools (`scrape.py` and `scrape.ipynb`) is a wrapper for the Python library [`twint`](https://github.com/twintproject/twint). Although Twint can be run on its own, the WE1S Scraper tool provides a handy notebook interface so that the user does not have to remember the Twint API.

*Notes on Twint:*

- Typical libraries that access the Twitter API require a Twitter developer account, which has become more complicated to obtain recently. The Twitter API also comes with limitations such as rate limits. Twint bypasses the Twitter API using [OSINT](https://en.wikipedia.org/wiki/Open-source_intelligence) tools to overcome these limitations.
- Twint is under active development, and frequent changes make installation strategies somewhat unreliable. We have found that the following commands work best:

```
pip install --user --upgrade -e git+https://github.com/twintproject/twint.git@origin/master#egg=twint

pip install nest_asyncio
```

## Preprocessor

The Preprocessor is a script that loads the Twint output and performs a variant of the WE1S preprocessing pipeline on the text of the tweets. The result is saved in a new `tidy_tweet` field. The preprocessing algorithm is described at the beginning of the notebook. The Preprocessor comes with a stoplist, which is a variant of the standard WE1S stoplist.
