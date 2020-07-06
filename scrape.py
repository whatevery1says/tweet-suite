"""scrape.py.

This script uses the Python [twint](https://github.com/twintproject/twint) library
to scrape Twitter. See the twint website for full documentation of its API.

Installing twint can be tricky. The following two commands (both required) worked
best at the time of writing.

pip install --user --upgrade -e git+https://github.com/twintproject/twint.git@origin/master#egg=twint
pip install nest_asyncio

Usage:

options = {'username': 'sekleinman'}
scraper = TwitterScraper(output_dir, output_format, options)
scraper.scrape()
"""

# Python imports
import datetime
import os
try:
    import twint
except ImportError:
    raise('Please install twint using the command `pip install --user --upgrade -e git+https://github.com/twintproject/twint.git@origin/master#egg=twint`.')
try:
    import nest_asyncio
    nest_asyncio.apply()
except ImportError:
    raise('Please install nest_asyncio using the command `pip install nest_asyncio`.')

# Classes
class TwitterScraper():
    """Scrape Twitter with Twint."""

    def __init__(self, output_dir, output_format, options):
        """Initialize the scraper.

        @output_dir (str): The file path to save the scraped data.
        @output_format (str): 'json' or 'csv' output. Default is 'json'.
        @options (dict): A dict containing any of the options below. See further
                         https://github.com/twintproject/twint/wiki/Configuration.
        Username             (string) - Twitter user's username
        User_id              (string) - Twitter user's user_id
        Search               (string) - Search terms
        Geo                  (string) - Geo coordinates (lat,lon,km/mi.)
        Location             (bool)   - Set to True to attempt to grab a Twitter user's location (slow).
        Near                 (string) - Near a certain City (Example: london)
        Lang                 (string) - Compatible language codes: https://github.com/twintproject/twint/wiki/Langauge-codes
        Year                 (string) - Filter Tweets before the specified year.
        Since                (string) - Filter Tweets sent since date, works only with twint.run.Search (Example: 2017-12-27).
        Until                (string) - Filter Tweets sent until date, works only with twint.run.Search (Example: 2017-12-27).
        Email                (bool)   - Set to True to show Tweets that _might_ contain emails.
        Phone                (bool)   - Set to True to show Tweets that _might_ contain phone numbers.
        Verified             (bool)   - Set to True to only show Tweets by _verified_ users
        Show_hashtags        (bool)   - Set to True to show hashtags in the terminal output.
        Limit                (int)    - Number of Tweets to pull (Increments of 20).
        Count                (bool)   - Count the total number of Tweets fetched.
        Stats                (bool)   - Set to True to show Tweet stats in the terminal output.
        To                   (string) - Display Tweets tweeted _to_ the specified user.
        All                  (string) - Display all Tweets associated with the mentioned user.
        Resume               (string) - Resume from the latest scroll ID, specify the filename that contains the ID.
        Images               (bool)   - Display only Tweets with images.
        Videos               (bool)   - Display only Tweets with videos.
        Media                (bool)   - Display Tweets with only images or videos.
        Lowercase            (bool)   - Automatically convert uppercases in lowercases.
        Retweets             (bool)   - Get retweets done by the user.
        Hide_output          (bool)   - Hide output.
        Popular_tweets       (bool)   - Scrape popular tweets, not most recent (default: False).
        Native_retweets      (bool)   - Filter the results for retweets only (warning: a few tweets will be returned!).
        Min_likes            (int)    - Filter the tweets by minimum number of likes.
        Min_retweets         (int)    - Filter the tweets by minimum number of retweets.
        Min_replies          (int)    - Filter the tweets by minimum number of replies.
        Links                (string) - Include or exclude tweets containing one o more links. If not specified you will get both tweets that might contain links or not. (please specify `include` or `exclude`)
        Source               (string) - Filter the tweets for specific source client. (example: `--source "Twitter Web Client"`)
        Members_list         (string) - Filter the tweets sent by users in a given list.
        Filter_retweets      (bool)   - Exclude retweets from the results.
        """
        self.config = twint.Config()
        for k, v in options.items():
            setattr(self.config, k.title(), v)
        # Override automatic configuration from options
        self.config.Format = '{date}: {tweet}'
        if output_format == 'csv':
            self.config.Store_csv = True
        else:
            self.config.Store_json = True
        self.output_dir = output_dir
        self.config.Output = self.output_dir
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
        print('Configuration complete. Run `scraper.scrape()` to begin scraping.')

    def scrape(self):
        """Exectute the scraper."""
        twint.run.Search(self.config)
