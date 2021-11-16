from tokenizer import Tokenizer
from indexer import Indexer

# tokenizer = Tokenizer(stopwords_path='', stemmer_enabled=True, size_filter=0)
tokenizer = Tokenizer()
indexer = Indexer(tokenizer)
indexer.index_data_source('content/data.tsv.gz')
index = indexer.get_memory_inverted_index()

# input_str = 'The quick brown fox things is played out'
# tokens = tokenizer.tokenize(input_str)
# indexer.index('ID2', tokens)