from tokenizer import Tokenizer
from indexer import Indexer
from filecmp import cmpfiles
from os import listdir, scandir, remove

test_file = 'amazon_reviews_us_Digital_Video_Games_v1_00_sample'

for file in scandir('index/' + test_file):
    remove(file)

tokenizer = Tokenizer(stopwords_path='', stemmer_enabled=True, size_filter=0,
                      use_positions=False)
indexer = Indexer(tokenizer, 30)
indexer.index_data_source('content/' + test_file + '.tsv.gz')

reference_file_list = listdir('index/' + test_file + '_reference/nonpositional')

matching_files, mismatching_files, error_files = cmpfiles(
    'index/' + test_file,
    'index/' + test_file + '_reference/nonpositional',
    reference_file_list, shallow=False)

assert len(mismatching_files) + len(error_files) == 0


for file in scandir('index/' + test_file):
    remove(file)

tokenizer = Tokenizer(stopwords_path='', stemmer_enabled=True, size_filter=0,
                      use_positions=True)
indexer = Indexer(tokenizer, 30)
indexer.index_data_source('content/' + test_file + '.tsv.gz')

reference_file_list = listdir('index/' + test_file + '_reference/positional')

matching_files, mismatching_files, error_files = cmpfiles(
    'index/' + test_file,
    'index/' + test_file + '_reference/positional',
    reference_file_list, shallow=False)

assert len(mismatching_files) + len(error_files) == 0
