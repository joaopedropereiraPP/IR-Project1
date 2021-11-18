from tokenizer import Tokenizer
from collections import defaultdict
from typing import DefaultDict, List, Dict
# from gzip import open
import gzip
from csv import reader, writer
from os import path

class Indexer:
    memory_index: DefaultDict[str, DefaultDict[str, List[int]]]
    memory_document_frequency: DefaultDict[str, int]
    tokenizer: Tokenizer
    statistics: Dict[str, int]
    doc_keys: Dict[int, str]
    nr_indexed_docs:int
    
    def __init__(self, tokenizer: Tokenizer) -> None:
        self.tokenizer = tokenizer
        self.memory_index = defaultdict(lambda: 
                                                 defaultdict(lambda: []))
        self.memory_document_frequency = defaultdict(int)
        self.statistics = dict.fromkeys([
                                         'Total indexing time (s)',
                                         'Total index size on disk (bytes)',
                                         'Vocabulary size',
                                         'Number of temporary index segments',
                                         'Time to start up index searcher (s)'
                                         ])
        self.doc_keys = {}
        self.nr_indexed_docs = 0
    
    def get_memory_index(self) -> Dict[str, Dict[str, List[int]]]:
        return self.memory_index

    def index_doc(self, doc_id, doc_body) -> None:
        tokens = self.tokenizer.tokenize(doc_body)
        
        for token in tokens:
            self.memory_index[token][doc_id] = tokens[token]
            self.memory_document_frequency[token] += 1

    # Temporary index blocks and the final index will be kept in the 'index'
    # subfolder
    def index_data_source(self, data_source_path: str) -> None:
        self.initialize_statistics()        
        
        with gzip.open(data_source_path, 
                  mode='rt', encoding='utf8', newline='') as data_file:
            data_file_name = path.basename(data_source_path).split('.')[0]
            data_reader = reader(data_file, delimiter='\t')
            
            # skip the first line (the header)
            data_file.readline()
            
            for doc in data_reader:
                
                # condition to dump index block to disk
                if len(self.memory_index.keys()) > 3:
                    # TODO: between runs of the same Indexer instance the 
                    # statistics will accumulate, this will need to be fixed
                    self.dump_index_to_disk(data_file_name)

                parsed_doc = self.parse_doc_from_data_source(doc)
                self.index_doc(parsed_doc[0], parsed_doc[1])
            
            # If the memory wasn't exceeded and the index isn't empty, make a
            # final dump to disk
            if len(self.memory_index.keys()) > 0:
                self.dump_index_to_disk(data_file_name)
            
            self.merge_index_blocks(data_file_name)
        
    # fields other than reviewid are concatenated separated by spaces, as the
    # body of the document
    def parse_doc_from_data_source(self, doc: List[str]) -> List[str]:
        doc_id = doc[2]
        doc_body = '{} {} {}'.format(doc[5], doc[12], doc[13])
        
        self.nr_indexed_docs += 1
        key = self.nr_indexed_docs
        self.doc_keys[key] = doc_id
 
        return [str(key), doc_body]

    def get_statistics(self) -> Dict[str, int]:
        return self.statistics
    
    def initialize_statistics(self) -> None:
        for key in self.statistics:
            self.statistics[key] = 0
    
    def dump_index_to_disk(self, data_file_name: str) -> None:
        self.statistics['Number of temporary index segments'] += 1
        
        block_file_name = '{}_block{:000}.tsv'.format(
            data_file_name, 
            self.statistics['Number of temporary index segments'])
        block_file_path = path.join('index', block_file_name)
            
        with open(block_file_path, mode='wt', encoding='utf8', 
                  newline='') as block_file:
            block_writer = writer(block_file, delimiter='\t')
            
            ordered_terms = list(self.memory_index.keys())
            list.sort(ordered_terms)
            for block_term in ordered_terms:
                block_writer.writerow(self.parse_term_from_memory(block_term))
        
        # self.memory_index.clear()
    
    def parse_term_from_memory(self, block_term):
        posting_list = [block_term]
        
        for posting in self.memory_index[block_term]:
            positions_str = ','.join([str(i) for i in self.memory_index[block_term][posting]])
            posting_list.append(posting + ':' + positions_str)
        
        return posting_list
        
    def merge_index_blocks(self, data_file_name: str) -> None:
        pass