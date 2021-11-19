from tokenizer import Tokenizer
from indexer import Indexer

tokenizer = Tokenizer(stopwords_path='', stemmer_enabled=False, size_filter=0,
                      use_positions=False)
indexer = Indexer(tokenizer)

docfreq = indexer.index_data_source('content/data.tsv.gz')

# indexer.index_doc('ID1', 'This is a test document, dummy!')
# indexer.index_doc('ID2', 'This is yet another document to test document indexing.')

# docfreq = indexer.merge_index_blocks('index/data')
# index = indexer.get_memory_index()
