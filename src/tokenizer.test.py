from tokenizer import Tokenizer
from indexer import Indexer

tokenizer = Tokenizer(stopwords_path='',stemmer_enabled=True, size_filter=0,
                      use_positions=True)
input_str = 'The quick brown fox jumped over the lazy brown dog.'
tokens = tokenizer.tokenize(input_str)
print(tokens)
