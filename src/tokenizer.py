from re import sub
from Stemmer import Stemmer
from collections import defaultdict
from typing import List, Dict, Set

class Tokenizer:
    stopwords_path: str
    stopwords: Set[str]
    stemmer_enabled: bool
    size_filter: int
    stemmer: Stemmer
    use_positions: bool
    
    # an empty string as a stopwords_path disables stopwords.
    # a size_filter of 0 disables size filter
    def __init__(self, stopwords_path: str = 'content/stopwords.txt',  stemmer_enabled: bool = True, size_filter: int = 3) -> None:
        
        self.stopwords_path = stopwords_path
        if stopwords_path != '':
            with open(stopwords_path, 'r') as stopwords_file:
                self.stopwords = set(stopwords_file.read().split('\n'))
        else:
            self.stopwords = set()

        self.stemmer_enabled = stemmer_enabled
        if stemmer_enabled:
            self.stemmer = Stemmer('english')
        
        self.size_filter = size_filter
    
    # makes a single iteration over all words to apply the stopword filter, the
    # size filter and the stemmer, to avoid multiple passes
    def tokenize(self, input_string: str) -> Set[str]:
        word_list = self.preprocess_input(input_string)
        tokens = set()

        for i in range(0, len(word_list)):
            if word_list[i] not in self.stopwords:
                if len(word_list[i]) > self.size_filter:
                    if self.stemmer_enabled:
                        token = self.stemmer.stemWord(word_list[i])
                    else:
                        token = word_list[i]
                    tokens.add(token)
        
        return tokens

    # similar to the tokenize method but also returns a list of positions
    # where each token was found
    def tokenize_positional(self, input_string: str) -> Dict[str, List[int]]:
        word_list = self.preprocess_input(input_string)
        tokens = defaultdict(lambda: [])

        for i in range(0, len(word_list)):
            if word_list[i] not in self.stopwords:
                if len(word_list[i]) > self.size_filter:
                    if self.stemmer_enabled:
                        token = self.stemmer.stemWord(word_list[i])
                    else:
                        token = word_list[i]
                    tokens[token].append(i)
        
        return tokens
    
    # the input string has all HTML line breaks and symbols replaced by spaces,
    # and words that start or end with numbers are then removed. It is then
    # made all lower case and split into substrings using the spaces to get
    # the words
    def preprocess_input(self, input_string: str) -> List[str]:
        word_list = sub("<br />|[^0-9a-zA-Z]+", " ", input_string)
        word_list = sub("[^a-zA-Z ]+[a-zA-Z]+|[a-zA-Z]+[^a-zA-Z ]+", "", word_list).lower().split(" ")
        
        return word_list
