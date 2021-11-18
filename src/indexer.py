from tokenizer import Tokenizer
from collections import defaultdict
from typing import DefaultDict, List, Dict
# from gzip import open
import gzip
from csv import reader, writer
from os import path

class Indexer:
    memory_index: DefaultDict[str, List[int]]
    memory_index_positional: DefaultDict[str, DefaultDict[str, List[int]]]
    memory_document_frequency: DefaultDict[str, int]
    
    tokenizer: Tokenizer
    doc_keys: Dict[int, str]
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
    
    def get_memory_index(self) -> Dict[str, Dict[str, List[int]]]:
        if self.tokenizer.use_positions:
            return self.memory_index_positional
        else:
            return self.memory_index

    def index_doc(self, doc_id, doc_body) -> None:
        tokens = self.tokenizer.tokenize(doc_body)
        
        for token in tokens:
            if self.tokenizer.use_positions:
                self.get_memory_index()[token][doc_id] = tokens[token]
            else:
                self.get_memory_index()[token].append(doc_id)
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
                if len(self.get_memory_index().keys()) > 3:
                    # TODO: between runs of the same Indexer instance the 
                    # statistics will accumulate, this will need to be fixed
                    self.dump_index_to_disk(data_file_name)

                doc_id, doc_body = self.parse_doc_from_data_source(doc)
                self.index_doc(doc_id, doc_body)
            
            # If the memory wasn't exceeded and the index isn't empty, make a
            # final dump to disk
            if len(self.get_memory_index().keys()) > 0:
                self.dump_index_to_disk(data_file_name)
            
            self.merge_index_blocks(data_file_name)

        self.dump_master_index()
        
    # fields other than reviewid are concatenated separated by spaces, as the
    # body of the document
    def parse_doc_from_data_source(self, doc: List[str]) -> (str, str):
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
        self.nr_indexed_docs = 0
        self.indexing_time = 0
        self.index_size = 0
        self.vocabulary_size = 0
        self.nr_temp_index_segments = 0
    
    # TODO: don't use data_file_name and instead use a folder and a
    # predetermined name for each index
    def dump_index_to_disk(self, data_file_name: str) -> None:
        self.nr_temp_index_segments += 1
        
        block_file_name = '{}_block{:000}.tsv'.format(
            data_file_name, 
            self.nr_temp_index_segments)
        block_file_path = path.join('index', block_file_name)
            
        with open(block_file_path, mode='wt', encoding='utf8', 
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
        
    def merge_index_blocks(self, data_file_name: str) -> None:
        pass

    def dump_master_index(self, index_folder_path = "index/master_index.tsv"):
        with open(index_folder_path, mode = 'wt', encoding = 'utf8') as master_file:
            for key in self.doc_keys:
                master_file.write("%s:%s\n"%(key,self.doc_keys[key]))