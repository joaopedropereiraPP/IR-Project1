from tokenizer import Tokenizer
from collections import defaultdict
from typing import DefaultDict, List, Dict, Tuple
# from gzip import open
import gzip
from csv import reader, writer
from os import path, makedirs

class Indexer:
    memory_index: DefaultDict[str, List[int]]
    memory_index_positional: DefaultDict[str, DefaultDict[str, List[int]]]
    memory_document_frequency: DefaultDict[str, int]
    doc_keys: Dict[int, str]
    
    tokenizer: Tokenizer
    
    nr_postings: int
    nr_indexed_docs: int
    indexing_time: int
    index_size: int
    vocabulary_size: int
    nr_temp_index_segments: int
    index_searcher_start_time: int
    
    def __init__(self, tokenizer: Tokenizer) -> None:
        self.tokenizer = tokenizer
        
        if self.tokenizer.use_positions:
            self.memory_index_positional = defaultdict(lambda: 
                                                       defaultdict(lambda: []))
        else:
            self.memory_index = defaultdict(list)
        
        self.memory_document_frequency = defaultdict(int)
        self.doc_keys = {}
        
        self.initialize_statistics()
        # TODO: make an initialization method to reset everything that can't
        # be reused so that the indexer can index multiple times with one 
        # instance
    
    def get_memory_index(self):
        if self.tokenizer.use_positions:
            return self.memory_index_positional
        else:
            return self.memory_index

    def index_doc(self, doc_id, doc_body) -> None:
        tokens = self.tokenizer.tokenize(doc_body)
        
        self.nr_postings += len(tokens)
        
        for token in tokens:
            if self.tokenizer.use_positions:
                self.get_memory_index()[token][doc_id] = tokens[token]
            else:
                self.get_memory_index()[token].append(doc_id)
            self.memory_document_frequency[token] += 1

    # All index files will be placed in the index/data_source_filename
    # subfolder
    def index_data_source(self, data_source_path: str) -> None:
        self.initialize_statistics()
        
        index_folder = 'index/' + path.basename(data_source_path).split('.')[0]
        if not path.exists(index_folder):
            makedirs(index_folder)
        
        with gzip.open(data_source_path, 
                  mode='rt', encoding='utf8', newline='') as data_file:
            data_reader = reader(data_file, delimiter='\t')
            
            # skip the first line (the header)
            data_file.readline()
            
            for doc in data_reader:
                
                # condition to dump index block to disk
                if self.nr_postings > 200:
                    
                    self.nr_temp_index_segments += 1
                    block_file_path = '{}/TempBlock{}.tsv'.format(
                        index_folder, 
                        self.nr_temp_index_segments)
                    self.dump_index_to_disk(block_file_path)

                doc_id, doc_body = self.parse_doc_from_data_source(doc)
                self.index_doc(doc_id, doc_body)
            
            # If the memory wasn't exceeded and the index isn't empty, make a
            # final dump to disk
            if len(self.get_memory_index().keys()) > 0:
                
                self.nr_temp_index_segments += 1
                block_file_path = '{}/TempBlock{}.tsv'.format(
                    index_folder, 
                    self.nr_temp_index_segments)
                self.dump_index_to_disk(block_file_path)
            
            self.merge_index_blocks(index_folder)

        self.dump_doc_keys(index_folder)
        
    # fields other than reviewid are concatenated separated by spaces, as the
    # body of the document
    def parse_doc_from_data_source(self, doc: List[str]) -> Tuple[str, str]:
        doc_id = doc[2]
        doc_body = '{} {} {}'.format(doc[5], doc[12], doc[13])
        
        self.nr_indexed_docs += 1
        self.doc_keys[self.nr_indexed_docs] = doc_id

        return self.nr_indexed_docs, doc_body

    def get_statistics(self) -> Dict[str, int]:
        statistics = {
            'Total indexing time (s)': self.indexing_time,
            'Total index size on disk (bytes)': self.index_size,
            'Vocabulary size': self.vocabulary_size,
            'Number of temporary index segments': self.nr_temp_index_segments,
        }

        return statistics
    
    def initialize_statistics(self) -> None:
        self.nr_postings = 0
        self.nr_indexed_docs = 0
        self.indexing_time = 0
        self.index_size = 0
        self.vocabulary_size = 0
        self.nr_temp_index_segments = 0
    
    # TODO: don't use data_file_name and instead use a folder and a
    # predetermined name for each index
    def dump_index_to_disk(self, file_path: str) -> None:
        with open(file_path, mode='wt', encoding='utf8', 
                  newline='') as block_file:
            block_writer = writer(block_file, delimiter='\t')
            
            ordered_terms = list(self.get_memory_index().keys())
            list.sort(ordered_terms)
            for block_term in ordered_terms:
                block_writer.writerow(
                    self.parse_index_term_for_dumping(block_term))
        
        self.get_memory_index().clear()
    
    def parse_index_term_for_dumping(self, term: str) -> List[str]:
        row = [term]
        
        for posting in self.get_memory_index()[term]:
            if self.tokenizer.use_positions:
                positions_str = ','.join([str(i) for i in 
                                  self.get_memory_index()[term][posting]])
                row.append(str(posting) + ':' + positions_str)
            else:
                row.append(str(posting))
        
        return row
        
    def merge_index_blocks(self, index_blocks_folder: str) -> None:
        pass

    # TODO: ordering?
    def dump_doc_keys(self, index_folder_path: str) -> None:
        file_path = index_folder_path + '/DocKeys.tsv'
        
        with open(file_path, mode='wt', encoding='utf8', 
                  newline='') as master_index_file:
            file_writer = writer(master_index_file, delimiter='\t')
            # ordered_terms = list(self.get_memory_index().keys())
            # list.sort(ordered_terms)
            for key in self.doc_keys:
                file_writer.writerow([key, self.doc_keys[key]])

    def read_index_from_disk(self, index_file_path: str):
        with open(index_file_path, 
                  mode='rt', encoding='utf8', newline='') as data_file:
            data_reader = reader(data_file, delimiter='\t')

            for row in data_reader:
                term = row[0]
                posting_str_list = row[1:]
                if self.tokenizer.use_positions:
                    for posting_str in posting_str_list:
                        doc_id, positions_str = posting_str.split(':')
                        positions_list = list(map(int, 
                                                  positions_str.split(',')))
                        self.memory_index_positional[term][doc_id] = positions_list
                else:
                    self.memory_index[term] = list(map(int, posting_str_list))
