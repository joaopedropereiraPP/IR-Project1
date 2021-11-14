from re import sub
from Stemmer import Stemmer
from os import path
from collections import defaultdict

class Tokenizer:
    stopwords: list
    stemmer_enabled: bool
    size_filter: int
    stemmer: Stemmer
    
    # an empty string as a stopwords_path disables stopwords
    # a size_filter of 0 disables size filter
    def __init__(self, stopwords_path: str = 'content/stopwords.txt', 
                 stemmer_enabled: bool = True, size_filter: int = 3):
        assert path.exists(stopwords_path) or stopwords_path == ''
        assert size_filter >= 0
        
        if stopwords_path != '':
            stopwords_file = open(stopwords_path,'r')
            self.stopwords = [word.strip() 
                              for word in stopwords_file.readlines()]
        else:
            self.stopwords = []

        if stemmer_enabled:
            self.stemmer_enabled = stemmer_enabled
            self.stemmer = Stemmer('english')
        
        self.size_filter = size_filter
        
    def tokenize(self, input_string: str):
        # dictionary of stopwords and list of respective positions in the 
        # document
        tokens = defaultdict(lambda: [])

        word_list = self.preprocess_input(input_string)
        
        # iterate over all words to fill the dictionary according to stopwords,
        # size filter and use of stemmer. The use of a single iteration for all
        # this further processing saves performance
        for i in range(0, len(word_list)):
            if word_list[i] not in self.stopwords:
                if len(word_list[i]) > self.size_filter:
                    if self.stemmer_enabled:
                        token = self.stemmer.stemWord(word_list[i])
                    else:
                        token = word_list[i]
                    tokens[token].append(i)
        
        return tokens
    
    # the input string will be made all lowercase and divided into a list of
    # terms every time a symbol that is not a letter or number appears.
    # hyphenated words will be joined in a single word
    def preprocess_input(self, input_string: str):
        word_list = sub("\-+","",input_string)
        word_list = sub("[^0-9a-zA-Z]+"," ",input_string).lower().split(" ")
        
        return word_list