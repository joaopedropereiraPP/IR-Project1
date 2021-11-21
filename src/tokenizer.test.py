from tokenizer import Tokenizer
from indexer import Indexer

# tokenizer = Tokenizer(stopwords_path = '', stemmer_enabled = True, 
#                       size_filter=0, use_positions=False)
tokenizer = Tokenizer(stopwords_path = '', stemmer_enabled = False, 
                      size_filter=0, use_positions=False)
# input_str = 'A simple phrase with word repetition to test both tokenization and word positions.'
input_str = '<br />and this'
tokens = tokenizer.tokenize(input_str)
print(tokens)
