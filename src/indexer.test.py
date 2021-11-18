from tokenizer import Tokenizer
from indexer import Indexer

tokenizer = Tokenizer(stopwords_path='', stemmer_enabled=False, size_filter=0,
                      use_positions=True)
# tokenizer = Tokenizer(use_positions=True)
indexer = Indexer(tokenizer)

# indexer.index_data_source('content/data.tsv.gz')
indexer.index_doc('ID1', 'This is a test document, dummy!')
indexer.index_doc('ID2', 'This is yet another document to test document indexing.')

index = indexer.get_memory_index()

# a=[1,2,3]
# e=[str(i) for i in a]
# b=','.join(str(a))
# c=str(a)
# d=','.join([str(i) for i in a])
# print(str(a))
# print(b)
