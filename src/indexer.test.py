from tokenizer import Tokenizer
from indexer import Indexer

# tokenizer = Tokenizer(stopwords_path='', stemmer_enabled=True, size_filter=0)
tokenizer = Tokenizer()
indexer = Indexer(tokenizer)
indexer.index_data_source('content/data.tsv.gz')
index = indexer.get_memory_inverted_index()

# import csv
# csvfile = open('here.tsv', mode='wt', encoding='utf8', newline='')
# writer = csv.writer(csvfile, delimiter='\t')
# writer.writerow('a')
# writer.writerow('a')
# csvfile.close()