"""preprocess.py.

Usage:

pp = TwitterPreprocesser(stoplist_file='twitter_stoplist.txt',
                         language_model='en_core_web_sm')
pp.preprocess(input_file, output_file)
"""

# Python imports
import re
import spacy
import unicodedata
import ujson as json
from bs4 import BeautifulSoup
from dateutil.parser import parse
from ftfy import fix_text
from spacy.language import Language
from spacy.lang.en.stop_words import STOP_WORDS
from spacy.symbols import ORTH, LEMMA, POS, TAG
from spacy.tokenizer import Tokenizer
from spacy.tokens import Token
from spacymoji import Emoji
from time import time

# Happy Emoticons
emoticons_happy = set([
    ':-)', ':)', ';)', ':o)', ':]', ':3', ':c)', ':>', '=]', '8)', '=)', ':}',
    ':^)', ':-D', ':D', '8-D', '8D', 'x-D', 'xD', 'X-D', 'XD', '=-D', '=D',
    '=-3', '=3', ':-))', ":'-)", ":')", ':*', ':^*', '>:P', ':-P', ':P', 'X-P',
    'x-p', 'xp', 'XP', ':-p', ':p', '=p', ':-b', ':b', '>:)', '>;)', '>:-)',
    '<3'
    ])
# Sad Emoticons
emoticons_sad = set([
    ':L', ':-/', '>:/', ':S', '>:[', ':@', ':-(', ':[', ':-||', '=L', ':<',
    ':-[', ':-<', '=\\', '=/', '>:(', ':(', '>.<', ":'-(", ":'(", ':\\', ':-c',
    ':c', ':{', '>:\\', ';('
    ])

# All Emoticons
EMOTICONS = emoticons_happy.union(emoticons_sad)

#Emoji patterns
emoji_pattern = re.compile('['
         u'\U0001F600-\U0001F64F'  # emoticons
         u'\U0001F300-\U0001F5FF'  # symbols & pictographs
         u'\U0001F680-\U0001F6FF'  # transport & map symbols
         u'\U0001F1E0-\U0001F1FF'  # flags (iOS)
         u'\U00002702-\U000027B0'
         u'\U000024C2-\U0001F251'
         ']+', flags=re.UNICODE)

# Handle lemmatization exceptions
LEMMATIZATION_CASES = {
    'humanities': [{ORTH: u'humanities', LEMMA: u'humanities', POS: u'NOUN', TAG: u'NNS'}]
}

# Classes

class TwitterPreprocessor():
    """Class for preprocessing tweets."""

    def __init__(self, stoplist_file=None, language_model='en_core_web_sm'):
        """Initialize the TwitterPreprocessor object."""
        timer = Timer()
        self.nlp = spacy.load(language_model)
        self.nlp.tokenizer = self.create_tokenizer()
        self.customize_lemmas()
        if stoplist_file is not None:
            self.load_custom_stoplist(stoplist_file)
        print('Preprocessor setup complete.')
        print('Time elapsed: %s' % timer.get_time_elapsed())

    def build_pipeline(self):
        """Build spaCy pipeline."""
        # Add spacymoji
        emoji = Emoji(self.nlp, merge_spans=False)
        self.nlp.add_pipe(emoji, first=True)
        # Add entity skipping
        self.nlp.add_pipe(self.skip_ents, after='ner')

    def create_tokenizer(self):
        """Create the custom tokenizer."""
        # contains the regex to match all sorts of urls:
        from spacy.lang.tokenizer_exceptions import URL_PATTERN

        # spacy defaults: when the standard behaviour is required, they
        # need to be included when subclassing the tokenizer
        prefix_re = spacy.util.compile_prefix_regex(Language.Defaults.prefixes)
        infix_re = spacy.util.compile_infix_regex(Language.Defaults.infixes)
        suffix_re = spacy.util.compile_suffix_regex(Language.Defaults.suffixes)

        # extending the default url regex with regex for hashtags with "or" = |
        hashtag_pattern = r'''|^(#[\w_-]+)$'''
        url_and_hashtag = URL_PATTERN + hashtag_pattern
        url_and_hashtag_re = re.compile(url_and_hashtag)

        # set a custom extension to match if token is a hashtag
        # hashtag_getter = lambda token: token.text.startswith('#')
        # Token.set_extension('is_hashtag', getter=hashtag_getter)
        return Tokenizer(self.nlp.vocab, prefix_search=prefix_re.search,
                        suffix_search=suffix_re.search,
                        infix_finditer=infix_re.finditer,
                        token_match=url_and_hashtag_re.match
                        )

    def customize_lemmas(self):
        """Add lemmatization special cases."""
        for k, v in LEMMATIZATION_CASES.items():
            self.nlp.tokenizer.add_special_case(k, v)

    def load_custom_stoplist(self, stoplist_file):
        """Load custom stoplist."""
        with open(stoplist_file, 'r') as f:
            stoplist = f.read().split('\n')
        for item in stoplist:
            STOP_WORDS.add(item)
            self.nlp.vocab[item].is_stop = True

    def preprocess(self, input_file, output_file):
        """Preprocess tweets from a file of line-delimited json objects.

        Saves the objects to a new file with a field called 'tidy_tweet'
        containing the preprocessed tweet.
        """
        timer = Timer()
        records = list(map(json.loads, open(input_file, encoding='utf-8')))
        with open(output_file, 'w') as f:
            for row in records:
                row['tidy_tweet'] = self.preprocess_tweet(row['tweet'])
                row['name'] = row['date'] + row['link'].replace('https://twitter.com/', '__').replace('/', '_')
                f.write(json.dumps(row) + '\n')
        print('Preprocessing complete.')
        print('Time elapsed: %s' % timer.get_time_elapsed())

    def preprocess_tweet(self, tweet):
        """Preprocess a single tweet."""
        tweet = fix_text(tweet, normalization='NFC')
        tweet = self.remove_accents(tweet, method='unicode')
        tweet = self.strip_html_tags(tweet).strip()
        doc = self.nlp(tweet)
        tokens = [token.norm_.strip().replace(' ', '_') for token in doc
                if not token._.is_emoji
                and token.text not in EMOTICONS
                and not token.is_stop
                and not token.is_punct
                and not token.is_quote
                and not token.is_space
                and not token.like_num
                and not token.like_url
                and not token.text.startswith('pic.twitter.com')
                and not token.ent_type_ == 'MONEY'
                and not token.ent_type_ == 'DATE'
                and not token.ent_type_ == 'TIME'
                and not token.ent_type_ == 'QUANTITY'
                and token.text != "'s"
                and len(token.text) > 1
                ]
        new_tokens = []
        for token in tokens:
            try:
                parse(token, fuzzy_with_tokens=True)
            except:
                new_tokens.append(token)
        return ' '.join(new_tokens)

    def remove_accents(self, text, method='unicode'):
        """Replace accents with unaccented letters"""
        if method == 'unicode':
            return ''.join(
                c
                for c in unicodedata.normalize('NFKD', text)
                if not unicodedata.combining(c)
            )
        elif method == 'ascii':
            return (
                unicodedata.normalize('NFKD', text)
                .encode('ascii', errors='ignore')
                .decode('ascii')
            )
        else:
            msg = '`method` must be either "unicode" and "ascii", not {}'.format(method)
            raise ValueError(msg)

    def skip_ents(self, doc, skip=['CARDINAL', 'DATE', 'QUANTITY', 'TIME']):
        """Handle entity merging in the tokenizer."""
        # Match months
        # months = re.compile(r'(?:jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sept(?:ember)?|oct(?:ober)?|nov(?:ember)?|Dec(?:ember)?)')
        with doc.retokenize() as retokenizer:
            for ent in doc.ents:
                merge = True
                if ent.label_ in skip:
                    merge = False
                # if ent.label_ == 'DATE' and re.match(months, ent.text.lower()):
                #    merge = True
                if merge == True:
                    attrs = {"tag": ent.root.tag, "dep": ent.root.dep, "ent_type": ent.label}
                    retokenizer.merge(ent, attrs=attrs)
        return doc

    def strip_html_tags(self, html):
        """Strip html tags."""
        if html is None:
            return None
        else:
            return ''.join(BeautifulSoup(html).findAll(text=True))

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
