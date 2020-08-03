"""aggregate.py.

Usage:

agg = TweetAggregator(input_file, output_dir)

View all records with `agg.records` or `agg.df`.

agg.aggregate_by_daterange(date_ranges, aggregate_title)

View result with `agg.aggregated_df`.

To filter the tweets:

agg.aggregate_by_filter(filter)

View the result with `agg.aggregated_df`.
"""

# Python imports
import os
import pandas as pd
import ujson as json
from time import time

class TweetAggregator():
    """Perform an aggregation of tweets and save as valid json files.

    @input_file (ndjson): Newline delimited json file with one tweet per object
    @output_dir: Directory to save the output
    @filter (str): The json field to aggregate the records on
    """

    def __init__(self,
                 input_file,
                 output_dir,
                 filter=None):
        """Initialize the TweetAggregator object."""
        # Make sure the output folder exists
        self.input_file = input_file
        self.output_dir = output_dir
        self.records = None
        self.df = None
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
        self.load_records()

    def aggregate_by_daterange(self, date_ranges, aggregate_title):
        """Aggregate by date range and save json files.

        @date_ranges (list): A list of tuples consisting of the start and
                             end dates of a date rang in YYYY-MM-DD format.
        @aggregate_title (str): The value to go in the manifest's title field.
        """
        timer = Timer()
        # For each date range, filter the dataframe and write it to a file
        for range in date_ranges:
            manifest = {}
            manifest['name'] = range[0] + '-' + range[1] + '_twitter_humanities'
            manifest['namespace'] = 'we1sv2.0'
            manifest['metapath'] = 'Corpus,twitter'
            manifest['title'] = aggregate_title
            manifest['date_range'] = {'start': range[0], 'end': range[1]}
            filename = range[0] + '-' + range[1] + '.json'
            aggregated_df = self.df.loc[self.df['date'].between(range[0], range[1], inclusive=True)]
            tweets = aggregated_df['tidy_tweet'].values.tolist()
            manifest['content'] = ' '.join(tweets)
            with open(os.path.join(self.output_dir, filename), 'w') as f:
                f.write(json.dumps(manifest, indent=2))
            print('Saved ' + filename)
        print('Aggregation complete.')
        print('Time elapsed: %s' % timer.get_time_elapsed())

    def aggregate_by_filter(self, filter):
        """Aggregate by filter and save json files.

        @filter (str): The column value to filter by.
        """
        timer = Timer()
        count = 0
        values = self.df[filter].values.tolist()
        values = sorted(list(set(values)))
        for value in values:
            filename = value + '.json'
            aggregated_df = self.df[self.df[filter] == value]
            if len(aggregated_df[filter]) > 1:
                count += 1
            aggregated_df.to_json(os.path.join(self.output_dir, filename), orient='records', lines=True)
            print('Saved ' + filename)
        print('Aggregation complete.')
        print('Time elapsed: %s' % timer.get_time_elapsed())

    def aggregate_multiple_tweeters(self, output_file, minimum_num_tweets=2, save=True):
        """Save a copy of the input file with only users with multiple tweets.

        @output_File (str): Path to the file to save to.
        @minimum_num_tweets (int): The minimum threshold of tweets per author.
        @save (bool): Whether or not to save the output file.
        """
        timer = Timer()
        multiple_tweeters = self.df.groupby('username').filter(lambda x: x['username'].count() >= minimum_num_tweets)
        if save == True:
            multiple_tweeters.to_json(output_file, orient='records', lines=True)
        else:
            return multiple_tweeters
        print('Aggregation complete.')
        print('Time elapsed: %s' % timer.get_time_elapsed())

    def aggregate_tweeters(self, df, output_file, file_suffix=None, group_by_col='username', join_col='tidy_tweet', save=False):
        """Save all tweets by each author into separate json files.

        @df (dataframe): The input dataframe.
        @output_file (str): The path to the file to be saved.
        @file_suffix (str): String to be added to the end of filenames (e.g. '_tweets_2014-2017').
        @group_by_col (str): The column on which the new dataframe will be grouped
        @join_col (str): The column for which the text in each item group will be joined
        @save (bool): Pass False to aggregate_multiple_tweeters() so that its output is not saved to file.

        Returns a dataframe with the group by column and the joined column.
        """
        timer = Timer()
        multiple_tweeters = self.aggregate_multiple_tweeters(output_file, minimum_num_tweets=1, save=False)
        grouped = multiple_tweeters.groupby(group_by_col)[join_col]
        aggregated_records = grouped.apply(lambda x: '%s' % ' '.join(x))
        for _, row in aggregated_records.iterrows():
            manifest = {'name': row['username'] + file_suffix, 'namespace': 'we1sv2.0', 'metapath': 'Projects'}
            manifest['content'] = row['tidy_tweet']
            fn = manifest['name'] + '.json'
            filepath = os.path.join(self.output_dir, fn)
            with open(filepath, 'w') as f:
                f.write(json.dumps(manifest, indent=2))
        print('Aggregation complete.')
        print('Time elapsed: %s' % timer.get_time_elapsed())

    def load_records(self):
        """Load the records from the input file."""
        timer = Timer()
        self.records = map(json.loads, open(self.input_file, encoding='utf-8'))
        self.df = pd.DataFrame.from_records(self.records)
        print('Records loaded.')
        print('Time elapsed: %s' % timer.get_time_elapsed())

class Timer:
    """Create a timer object."""

    def __init__(self):
        """Initialise the timer object."""
        self.start = time()

    def restart(self):
        """Restart the timer."""
        self.start = time()

    def get_time_elapsed(self):
        """Get the elapsed time and format it as hours, minutes, and seconds."""
        end = time()
        m, s = divmod(end - self.start, 60)
        h, m = divmod(m, 60)
        time_str = "%02d:%02d:%02d" % (h, m, s)
        return time_str
