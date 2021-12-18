import gzip
from collections import defaultdict
from contextlib import ExitStack
from csv import QUOTE_NONE, field_size_limit, reader, unix_dialect, writer
from glob import glob
from os import makedirs, path
from time import time
from typing import DefaultDict, Dict, List, Tuple
from bm25 import bm25
from math import log10

from tokenizer import Tokenizer


class Indexer:
    tokenizer: Tokenizer
    max_postings_per_temp_block: int
    index_type: str

    block_posting_count: int

    # the positional inverted index needs a different structure to store
    # positions
    inverted_index: DefaultDict[str, List[int]]
    inverted_index_positional: DefaultDict[str, DefaultDict[str, List[int]]]

    # contains for each term its document frequency and file number of the
    # final index blocks
    master_index: DefaultDict[str, List[int]]

    # contains the correspondence of surrogate keys to natural keys (the
    # hexadecimal keys from the data source)
    doc_keys: Dict[int, str]

    # statistics
    nr_postings: int
    nr_indexed_docs: int
    indexing_time: int
    index_size: int
    vocabulary_size: int
    nr_temp_index_segments: int

    def __init__(self, tokenizer: Tokenizer, max_postings_per_temp_block: int = 1000000, index_type: str = 'raw') -> None:
        field_size_limit(10000000)
        self.tokenizer = tokenizer
        self.max_postings_per_temp_block = max_postings_per_temp_block
        self.block_posting_count = 0

        self.index_type = index_type
        self.initialize_index()

        self.master_index = defaultdict(lambda: [0, 0])
        self.doc_keys = {}

        self.initialize_statistics()

        self.k = 1.2
        self.b = 0.75
        self.to_merge = False

        self.logarithm = {}
        self.avdl = 0
        

    def get_inverted_index(self):
        if self.index_type == 'raw':
            if self.tokenizer.use_positions:
                return self.inverted_index_positional
            else:
                return self.inverted_index
        elif self.index_type == 'lnc.ltc':
            if self.tokenizer.use_positions:
                pass
            else:
                pass
        elif self.index_type == 'bm25':
            # inverted_index_positional is required for bm25 method
            return self.inverted_index_positional


    def initialize_statistics(self) -> None:
        self.nr_postings = 0
        self.nr_indexed_docs = 0
        self.indexing_time = 0
        self.index_size = 0
        self.vocabulary_size = 0
        self.nr_temp_index_segments = 0

    def get_statistics(self) -> Dict[str, int]:
        statistics = {
            'Number of indexed documents': self.nr_indexed_docs,
            'Number of postings': self.nr_postings,
            'Vocabulary size': self.vocabulary_size,
            'Total indexing time (s)': self.indexing_time,
            'Total index size on disk (MB)': self.index_size,
            'Number of temporary index segments': self.nr_temp_index_segments,
        }

        return statistics

    # tokenize and index document
    def parse_datasource_doc_to_memory(self, doc_id, doc_body) -> None:
        tokens = self.tokenizer.tokenize(doc_body)
        
        nr_tokens = len(tokens)
        self.nr_postings += nr_tokens
        self.block_posting_count += nr_tokens


        for token in tokens:

            if self.index_type == 'raw':
                if self.tokenizer.use_positions:
                    self.get_inverted_index()[token][doc_id] = tokens[token]
                else:
                    self.get_inverted_index()[token].append(doc_id)
            elif self.index_type == 'lnc.ltc':
                if self.tokenizer.use_positions:
                    pass
                else:
                    pass
            elif self.index_type == 'bm25':
                # positions required
                self.get_inverted_index()[token][doc_id] = tokens[token]

    def measure_index_file_size(self, index_folder: str):
        file_list = glob(index_folder + '/PostingIndexBlock*.tsv')
        file_list.append(index_folder + '/MasterIndex.tsv')
        file_list.append(index_folder + '/DocKeys.tsv')
        for file_path in file_list:
            self.index_size += path.getsize(file_path)
        self.index_size = self.index_size / 1000000.0

    # start indexing a data source, the index files will be placed in the
    # index/data_source_filename subfolder
    def index_data_source(self, data_source_path: str) -> None:
        self.initialize_statistics()

        start_time = time()

        index_folder = 'index/' + path.basename(data_source_path).split('.')[0]
        if not path.exists(index_folder):
            makedirs(index_folder)

        #metadados
        self.configurations(index_folder)

        # define the dialect used by csv.reader to correctly interpret amazon
        # review data files
        dialect = unix_dialect()
        dialect.delimiter = '\t'
        dialect.quoting = QUOTE_NONE
        dialect.escapechar = None

        with gzip.open(data_source_path,
                       mode='rt', encoding='utf8', newline='') as data_file:
            data_reader = reader(data_file, dialect)

            # skip the first line (the header)
            data_file.readline()

            for doc in data_reader:

                # index document to memory
                doc_id, doc_body = self.parse_doc_from_data_source(doc)
                self.parse_datasource_doc_to_memory(doc_id, doc_body)

                # dump temporary index block to disk if maximum postings limit
                # is exceeded
                if self.block_posting_count > self.max_postings_per_temp_block:

                    self.nr_temp_index_segments += 1
                    block_file_path = '{}/TempBlock{}.tsv'.format( index_folder, self.nr_temp_index_segments)
                    self.dump_index_to_disk(block_file_path)

            # if the maximum wasn't exceeded and the index isn't empty, make a
            # final dump to disk
            if len(self.get_inverted_index().keys()) > 0:

                self.nr_temp_index_segments += 1
                block_file_path = '{}/TempBlock{}.tsv'.format(
                    index_folder,
                    self.nr_temp_index_segments)
                self.dump_index_to_disk(block_file_path)

        # update self.avdl
        self.calculate_avdl()

        self.merge_index_blocks(index_folder)

        self.dump_master_index(index_folder)

        self.dump_doc_keys(index_folder)

        end_time = time()

        self.indexing_time = end_time - start_time

        self.measure_index_file_size(index_folder)

        self.vocabulary_size = len(self.doc_keys)

    # merge temporary index blocks and create the final index blocks
    def merge_index_blocks(self, index_blocks_folder: str) -> None:
        file_path_list = []
        last_term_list = []

        # temporary dictionary of terms that were read from each temporary
        # index block. The key is the block number while the value is its index
        # as a dictionary
        temp_merge_dict = defaultdict(dict)
        
        # prepare list of block file paths
        for block_number in range(1, self.nr_temp_index_segments + 1):
            file_path_list.append('{}/TempBlock{}.tsv'.format(index_blocks_folder, block_number))

        self.to_merge = True
        with ExitStack() as stack:

            block_files = [stack.enter_context(open(file_path))
                           for file_path in file_path_list]
            file_readers = [reader(block_file, delimiter='\t')
                            for block_file in block_files]

            # the maximum number of postings read per block is 70% of the total
            # divided by the number of temporary index blocks
            max_postings_read_per_block = int(
                (self.max_postings_per_temp_block
                    / self.nr_temp_index_segments) * 0.7)

            nr_final_index_blocks = 1
            self.block_posting_count = 0
            nr_merged_postings = 0

            while(nr_merged_postings < self.nr_postings):

                last_term_list.clear()

                # read terms from each temporary index block until the maximum
                # number of postings is exceeded
                for block_nr in range(0, self.nr_temp_index_segments):

                    nr_postings_read = 0
                    while nr_postings_read < max_postings_read_per_block:
                        try:
                            row = next(file_readers[block_nr])
                        except StopIteration:
                            break
                        term, value = self.parse_disk_term_to_memory(row)
                        temp_merge_dict[block_nr + 1][term] = value
                        nr_postings_read += len(value)

                    last_term_list.append(term)

                # of the last terms read on each block, the lexicographically
                # lowest term will be the last to be merged, while the others
                # remain in memory for the next iteration
                last_term_list.sort()
                last_term_to_merge = last_term_list[0]

                # merge on memory all terms in the temporary dictionary of read
                # terms from each block that are ready to be merged
                for block_nr in range(1, self.nr_temp_index_segments + 1):

                    block_terms = list(temp_merge_dict[block_nr].keys())

                    for term in block_terms:

                        if term <= last_term_to_merge:

                            nr_postings_for_term = len(
                                temp_merge_dict[block_nr][term])
                            self.block_posting_count += nr_postings_for_term
                            postings = temp_merge_dict[block_nr].pop(term)
                            self.merge_terms_in_memory(term, postings)

                            nr_merged_postings += nr_postings_for_term
                            self.add_term_to_master_index(
                                term, nr_postings_for_term,
                                nr_final_index_blocks)

                # dump to disk if the number of postings on the final index on
                # memory exceeds the maximum per block
                if self.block_posting_count >= self.max_postings_per_temp_block:
                    block_file_path = '{}/PostingIndexBlock{}.tsv'.format(
                        index_blocks_folder,
                        nr_final_index_blocks)
                    self.dump_index_to_disk(block_file_path)
                    nr_final_index_blocks += 1

            # if the maximum wasn't exceeded and the index isn't empty, make a
            # final dump to disk
            if len(self.get_inverted_index().keys()) > 0:
                block_file_path = '{}/PostingIndexBlock{}.tsv'.format(
                    index_blocks_folder,
                    nr_final_index_blocks)
                self.dump_index_to_disk(block_file_path)

    # add index file to memory
    def read_index_from_disk(self, index_file_path: str):
        with open(index_file_path,
                  mode='rt', encoding='utf8', newline='') as data_file:
            data_reader = reader(data_file, delimiter='\t')

            for row in data_reader:
                term, value = self.parse_disk_term_to_memory(row)
                self.get_inverted_index()[term] = value

    # process the contents of an index file for indexing in memory again
    def parse_disk_term_to_memory(self, row: List[str]):
        term = row[0]
        posting_str_list = row[1:]

        if self.index_type == 'raw':
            if self.tokenizer.use_positions:
                value = defaultdict(lambda: [])
                for posting_str in posting_str_list:
                    doc_id, positions_str = posting_str.split(':')
                    positions_list = list(map(int, positions_str.split(',')))
                    value[doc_id] = positions_list
            else:
                value = list(map(int, posting_str_list))
        elif self.index_type == 'lnc.ltc':
            if self.tokenizer.use_positions:
                pass
            else:
                pass
        elif self.index_type == 'bm25':
            # Required Positions
            value = defaultdict(lambda: [])
            for posting_str in posting_str_list:
                doc_id, positions_str = posting_str.split(':')
                positions_list = list(map(int, positions_str.split(',')))
                value[doc_id] = positions_list

        return term, value

    # process the contents of an Amazon review data file for indexing.
    # Fields other than reviewid are concatenated separated by spaces, as the
    # body of the document
    def parse_doc_from_data_source(self, doc: List[str]) -> Tuple[str, str]:
        doc_id = doc[2]
        structure_doc_keys= []
        doc_body = '{} {} {}'.format(doc[5], doc[12], doc[13])
        
        tokens = self.tokenizer.tokenize(doc_body)
        size = 0
        for term in tokens:
            size += len(tokens[term])

        #structure_doc_keys [doc_id, doc_name, document_length]
        structure_doc_keys.append(doc_id)
        structure_doc_keys.append(doc[5])
        structure_doc_keys.append(size)
        self.nr_indexed_docs += 1
        self.doc_keys[self.nr_indexed_docs] = structure_doc_keys

        return self.nr_indexed_docs, doc_body

    # process the contents of a term from the index on memory for storing on
    # disk
    def parse_memory_term_to_disk(self, term: str) -> List[str]:
        row = [term]
        for posting in self.get_inverted_index()[term]:
            print(term)
            if self.index_type == 'raw':
                if self.tokenizer.use_positions:
                    positions_str = (','.join( [str(i) for i in self.get_inverted_index()[term][posting]]))
                    row.append(str(posting) + ':' + positions_str)
                else:
                    row.append(str(posting))
            elif self.index_type == 'lnc.ltc':
                if self.tokenizer.use_positions:
                    pass
                else:
                    pass
            elif self.index_type == 'bm25':
                if self.to_merge:
                    #PROCESSING DATA TO MERGE WITH BM25
                    if self.tokenizer.use_positions:
                        print(posting)
                        dl = self.get_number_of_words_from_dockeys(int(posting))
                        bm25_procedure = bm25(k = self.k, b= self.b, positions= self.get_inverted_index()[term][posting], dl=dl, avdl= self.avdl)
                        row.append(str(posting) + ':' + bm25_procedure.bm25_with_positions())
                    else:
                        dl = self.get_number_of_words_from_dockeys(int(posting))
                        bm25_procedure = bm25(k = self.k, b= self.b, positions= self.get_inverted_index()[term][posting], dl=dl, avdl= self.avdl)
                        row.append(str(posting) + ':' + bm25_procedure.bm25_without_positions())
                else:
                    #PROCESSING DATA TO TEMPBLOCK
                    
                    positions_str = (','.join( [str(i) for i in self.get_inverted_index()[term][posting]]))
                    row.append(str(posting) + ':' + positions_str)
                    
        print("--------------------")
        return row

    # the resulting TSV file on disk will have a term on the first column of
    # each row, and a posting on each subsequent column, as its document ID.
    # When positions are being considered for the index, each posting will be a
    # string containing the document ID followed by the character ':' and the
    # list of positions on the document separated by ','
    def dump_index_to_disk(self, file_path: str) -> None:
        with open(file_path, mode='wt', encoding='utf8', newline='') as block_file:
            block_writer = writer(block_file, delimiter='\t')

            ordered_terms = list(self.get_inverted_index().keys())
            list.sort(ordered_terms)
            for block_term in ordered_terms:
                block_writer.writerow(self.parse_memory_term_to_disk(block_term))

        self.block_posting_count = 0

        self.get_inverted_index().clear()

    # the resulting TSV file on disk will have a term on the first column of
    # each row, followed on each column by its document frequency and the block
    # number of the final index where it can be found
    def dump_master_index(self, index_folder_path):
        file_path = index_folder_path + '/MasterIndex.tsv'

        with open(file_path, mode='wt', encoding='utf8', newline='') as master_index_file:
            file_writer = writer(master_index_file, delimiter='\t')
            keys = list(self.master_index.keys())
            list.sort(keys)
            keys.sort()
            for key in keys:
                r = self.log(self.nr_indexed_docs) - self.log(self.master_index[key][0])
                file_writer.writerow([key, r, self.master_index[key][1]])

    # the resulting TSV file on disk will have the surrogate key on each row,
    # followed by the natural key (hexadecimal) on the next column
    def dump_doc_keys(self, index_folder_path: str) -> None:
        file_path = index_folder_path + '/DocKeys.tsv'

        with open(file_path, mode='wt', encoding='utf8', newline='') as doc_keys_file:
            file_writer = writer(doc_keys_file, delimiter='\t')
            # ordered_terms = list(self.get_inverted_index().keys())
            # list.sort(ordered_terms)
            for key in self.doc_keys:
                file_writer.writerow([key, self.doc_keys[key][0], self.doc_keys[key][1], self.doc_keys[key][2]])

    def merge_terms_in_memory(self, term, postings):
        if self.index_type == 'raw':
            if self.tokenizer.use_positions:
                self.get_inverted_index()[term].update(postings)
            else:
                self.get_inverted_index()[term] += postings
        elif self.index_type == 'lnc.ltc':
            if self.tokenizer.use_positions:
                pass
            else:
                pass
        elif self.index_type == 'bm25':
            #Required positions
            #if self.tokenizer.use_positions:
            self.get_inverted_index()[term].update(postings)
        

    def add_term_to_master_index(self, term, nr_postings_for_term, nr_final_index_blocks):

        if self.index_type == 'raw':
            self.master_index[term][0] += nr_postings_for_term
            self.master_index[term][1] = nr_final_index_blocks
        elif self.index_type == 'lnc.ltc':
            pass
        elif self.index_type == 'bm25':
            self.master_index[term][0] += nr_postings_for_term
            self.master_index[term][1] = nr_final_index_blocks

    def initialize_index(self):
        if self.tokenizer.use_positions:
            self.inverted_index_positional = (defaultdict(lambda: defaultdict(lambda: [])))
        else:
            if self.index_type == 'raw' :
                self.inverted_index = defaultdict(list)
            elif self.index_type == 'bm25':
                # inverted_index_positional is required to bm25 method
                self.inverted_index_positional = (defaultdict(lambda: defaultdict(lambda: [])))
            else:
                self.inverted_index = defaultdict(float)

    def calculate_avdl(self):
        sum_of_documents_length = 0
        ndocs = 0
        for docs in self.doc_keys.keys():
            sum_of_documents_length += self.doc_keys[int(docs)][2]
            ndocs +=1
        self.avdl = sum_of_documents_length / ndocs


    def get_number_of_words_from_dockeys(self, document):
        return self.doc_keys[document][2]

    def log(self, n):
        if n not in self.logarithm:
            self.logarithm[n] = log10(n)
        return self.logarithm[n]

    def configurations(self, index_folder_path):
        file_path = index_folder_path + '/conf.tsv'

        with open(file_path, mode='wt', encoding='utf8', newline='') as conf_file:
            file_writer = writer(conf_file, delimiter='\t')
            
            file_writer.writerow(["Index Type",  str(self.tokenizer.index_type)])
            file_writer.writerow(["Size Filter",  str(self.tokenizer.size_filter)])
            file_writer.writerow(["Use Stemmer",  str(self.tokenizer.stemmer_enabled)])
            file_writer.writerow(["Stopword path",  str(self.tokenizer.stopwords_path)])
            file_writer.writerow(["Use Positions",  str(self.tokenizer.use_positions)])
