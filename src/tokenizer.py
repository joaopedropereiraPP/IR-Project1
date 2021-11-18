from re import sub
from Stemmer import Stemmer
from os import path
from collections import defaultdict
from typing import List, Dict

class Tokenizer:
    stopwords: List[str]
    stemmer_enabled: bool
    size_filter: int
    stemmer: Stemmer
    
    # An empty string as a stopwords_path disables stopwords.
    # A size_filter of 0 disables size filter
    def __init__(self, stopwords_path: str = 'content/stopwords.txt', 
                 stemmer_enabled: bool = True, size_filter: int = 3,
                 use_positions: bool = False) -> None:
        
        if stopwords_path != '':
            stopwords_file = open(stopwords_path, 'r')
            self.stopwords = [word.strip() 
                              for word in stopwords_file.readlines()]
        else:
            self.stopwords = []

        self.stemmer_enabled = stemmer_enabled
        if stemmer_enabled:
            self.stemmer = Stemmer('english')
        
        self.size_filter = size_filter
        
        self.use_positions = use_positions
        
    def tokenize(self, input_string: str) -> List[str]:
        word_list = self.preprocess_input(input_string)
        
        tokens = self.stem_and_filter_words(word_list)
        
        return tokens
    
    # The input string will be made all lowercase and divided into a list of
    # terms every time a symbol that is not a letter or number appears.
    # Hyphenated words will be joined in a single word
    def preprocess_input(self, input_string: str) -> List[str]:
        word_list = sub("\-+","", input_string)
        word_list = sub("[^0-9a-zA-Z]+"," ", word_list).lower().split(" ")
        
        return word_list
    
    # Returns a dictionary of tokens containing a list of respective positions
    # in the document.
    # It makes a single iteration over all words to apply the stopword filter, 
    # the size filter and the stemmer, to avoid multiple passes
    def stem_and_filter_words(
                      self, word_list: List[str]) -> List[str]:
        tokens = []

        for i in range(0, len(word_list)):
            if word_list[i] not in self.stopwords:
                if len(word_list[i]) > self.size_filter:
                    if self.stemmer_enabled:
                        token = self.stemmer.stemWord(word_list[i])
                    else:
                        token = word_list[i]
                    tokens.append(token)
        
        return tokens
    
    # Returns a dictionary of stopwords containing a list of respective 
    # positions in the document.
    # It makes a single iteration over all words to apply the stopword filter, 
    # the size filter and the stemmer, to avoid multiple passes
    def stem_and_filter_words_with_positions(
                      self, word_list: List[str]) -> Dict[str, List[int]]:
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
