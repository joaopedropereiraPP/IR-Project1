from tokenizer import Tokenizer
from indexer import Indexer

tokenizer = Tokenizer(stopwords_path='', stemmer_enabled=False, size_filter=0,
                      use_positions=False)
indexer = Indexer(tokenizer)

indexer.index_data_source('content/data.tsv.gz')

# indexer.index_doc('ID1', 'This is a test document, dummy!')
# indexer.index_doc('ID2', 'This is yet another document to test document indexing.')

# indexer.read_index_from_disk('index/data/TempBlock1.tsv')
# index = indexer.get_memory_index()
# index1 = indexer.memory_index
# index2 = indexer.memory_index_positional

# a=300
# b=3
# c=0.7
# print(int(a/b*c))
# print(-(-3//2))

# from collections import defaultdict
# a = defaultdict(dict)
# a[1][2]='a'

mergeindex = indexer.merge_index_blocks('index/data')
index = indexer.get_memory_index()
