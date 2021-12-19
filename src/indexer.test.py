from filecmp import cmpfiles
from os import listdir, path, remove, scandir

from indexer import Indexer
from tokenizer import Tokenizer


def indexer_test(indexer, test_file, reference_index_folder):

    test_file_basename = path.splitext(
        path.splitext(path.basename(test_file))[0])[0]
    test_index_folder = 'index/' + test_file_basename

    if path.exists(test_index_folder):
        for file in scandir(test_index_folder):
            remove(file)
    indexer.index_data_source(test_file)

    reference_file_list = listdir(reference_index_folder)

    matching_files, mismatching_files, error_files = cmpfiles(
        test_index_folder,
        reference_index_folder,
        reference_file_list, shallow=False)

    assert len(mismatching_files) + len(error_files) == 0


# raw index unit test
tokenizer = Tokenizer(stopwords_path='', stemmer_enabled=True, size_filter=0,
                      use_positions=False)
indexer = Indexer(tokenizer, 30)

test_file = 'content/amazon_reviews_us_Digital_Video_Games_v1_00_sample.tsv.gz'
reference_index_folder = 'index/amazon_reviews_us_Digital_Video_Games' + \
    '_v1_00_sample_reference/nonpositional'

indexer_test(indexer, test_file, reference_index_folder)


# positional raw index unit test
tokenizer = Tokenizer(stopwords_path='', stemmer_enabled=True, size_filter=0,
                      use_positions=True)
indexer = Indexer(tokenizer, 30)

test_file = 'content/amazon_reviews_us_Digital_Video_Games_v1_00_sample.tsv.gz'
reference_index_folder = 'index/amazon_reviews_us_Digital_Video_Games' + \
    '_v1_00_sample_reference/positional'

indexer_test(indexer, test_file, reference_index_folder)


# BM25 index unit test
tokenizer = Tokenizer(stopwords_path='content/stopwords.txt',
                      stemmer_enabled=True, size_filter=3, use_positions=True,
                      index_type='bm25')
indexer = Indexer(tokenizer, index_type='bm25')

test_file = 'content/data1.tsv.gz'
# indexer.index_data_source(test_file)
reference_index_folder = 'index/data1_reference'

indexer_test(indexer, test_file, reference_index_folder)
