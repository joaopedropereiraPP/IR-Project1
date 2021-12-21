
from filecmp import cmpfiles
from os import listdir, path, remove, scandir

from indexer import Indexer
from tokenizer import Tokenizer
from indexer_bm25 import IndexerBM25
from indexer_lnc_ltc import IndexerLncLtc


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


# # nonpositional raw index unit test
# tokenizer = Tokenizer(stopwords_path='', stemmer_enabled=True, size_filter=0)
# indexer = Indexer(tokenizer, 30)

# test_file = 'content/amazon_reviews_us_Digital_Video_Games_v1_00_sample.tsv.gz'
# reference_index_folder = 'index/amazon_reviews_us_Digital_Video_Games' + \
#     '_v1_00_sample_reference/nonpositional'

# indexer_test(indexer, test_file, reference_index_folder)


# # positional raw index unit test
# tokenizer = Tokenizer(stopwords_path='', stemmer_enabled=True, size_filter=0)
# indexer = Indexer(tokenizer, 30, use_positions=True)

# test_file = 'content/amazon_reviews_us_Digital_Video_Games_v1_00_sample.tsv.gz'
# reference_index_folder = 'index/amazon_reviews_us_Digital_Video_Games' + \
#     '_v1_00_sample_reference/positional'

# indexer_test(indexer, test_file, reference_index_folder)


# # nonpositional BM25 weighted index unit test
# tokenizer = Tokenizer(stopwords_path='content/stopwords.txt',
#                       stemmer_enabled=True, size_filter=3)
# indexer = IndexerBM25(tokenizer, use_positions=False)

# test_file = 'content/data1.tsv.gz'
# # indexer.index_data_source(test_file)
# reference_index_folder = 'index/data1_reference/nonpositional'

# indexer_test(indexer, test_file, reference_index_folder)


# # positional BM25 weighted index unit test
# tokenizer = Tokenizer(stopwords_path='content/stopwords.txt',
#                       stemmer_enabled=True, size_filter=3)
# indexer = IndexerBM25(tokenizer, use_positions=True)

# test_file = 'content/data1.tsv.gz'
# reference_index_folder = 'index/data1_reference/positional'

# indexer_test(indexer, test_file, reference_index_folder)

# nonpositional lnc.ltc weighted index unit test
tokenizer = Tokenizer(stopwords_path='content/stopwords.txt',
                      stemmer_enabled=True, size_filter=3)
indexer = IndexerLncLtc(tokenizer, use_positions=False)

test_file = 'content/data1.tsv.gz'
reference_index_folder = 'index/data1_reference/positional'

indexer.index_data_source(test_file)
# indexer_test(indexer, test_file, reference_index_folder)
