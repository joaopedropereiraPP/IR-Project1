from tokenizer import Tokenizer
from indexer import Indexer

# tokenizer = Tokenizer(stopwords_path='', stemmer_enabled=True, size_filter=0)
tokenizer = Tokenizer()
indexer = Indexer(tokenizer)
indexer.index_data_source('content/data.tsv.gz')
index = indexer.get_memory_inverted_index()

# a=[1,2,3]
# e=[str(i) for i in a]
# b=','.join(str(a))
# c=str(a)
# d=','.join([str(i) for i in a])
# print(str(a))
# print(b)
