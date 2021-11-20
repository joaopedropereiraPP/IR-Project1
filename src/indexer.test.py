from tokenizer import Tokenizer
from indexer import Indexer

# tokenizer = Tokenizer(stopwords_path='', stemmer_enabled=True, size_filter=0,
#                       use_positions=False)
tokenizer = Tokenizer()

# indexer = Indexer(tokenizer, 30)
# docfreq = indexer.index_data_source('content/data.tsv.gz')

indexer = Indexer(tokenizer, 1000000)
docfreq = indexer.index_data_source('content/amazon_reviews_us_Digital_Video_Games_v1_00.tsv.gz')

# indexer = Indexer(tokenizer, 1000000)
# docfreq = indexer.index_data_source('content/amazon_reviews_us_Digital_Music_Purchase_v1_00.tsv.gz')

# indexer = Indexer(tokenizer, 1000000)
# docfreq = indexer.index_data_source('content/amazon_reviews_us_Music_v1_00.tsv.gz')

# indexer = Indexer(tokenizer, 1000000)
# docfreq = indexer.index_data_source('content/amazon_reviews_us_Books_v1_00.tsv.gz')

# indexer.index_doc('ID1', 'This is a test document, dummy!')
# indexer.index_doc('ID2', 'This is yet another document to test document indexing.')
