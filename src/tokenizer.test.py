from tokenizer import Tokenizer
from indexer import Indexer

tokenizer = Tokenizer(stopwords_path = '', stemmer_enabled = True, 
                      size_filter=0, use_positions=True)
input_str = 'A simple phrase with word repetition to test both tokenization and word positions.'
tokens = tokenizer.tokenize(input_str)
print(tokens)
