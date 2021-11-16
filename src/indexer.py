from tokenizer import Tokenizer
from collections import defaultdict
from typing import List, Dict
from gzip import open
from csv import reader, writer
from os import path

class Indexer:
    memory_inverted_index: Dict[str, Dict[str, List[int]]]
    memory_document_frequency: Dict[str, int]
    tokenizer: Tokenizer
    statistics: Dict[str, int]
    
    def __init__(self, tokenizer: Tokenizer) -> None:
        self.tokenizer = tokenizer
        self.memory_inverted_index = defaultdict(lambda: 
                                                 defaultdict(lambda: []))
        self.memory_document_frequency = defaultdict(int)
        self.statistics = dict.fromkeys([
                                         'Total indexing time (s)',
                                         'Total index size on disk (bytes)',
                                         'Vocabulary size',
                                         'Number of temporary index segments',
                                         'Time to start up index searcher (s)'
                                         ], 0)
    
    def get_memory_inverted_index(self) -> Dict[str, Dict[str, List[int]]]:
        return self.memory_inverted_index

    def index_doc(self, doc_id, tokens):
        for token in tokens:
            
            self.memory_inverted_index[token][doc_id] = tokens[token]
            self.memory_document_frequency[token] += 1

    # Temporary index blocks and the final index will be kept in the 'index'
    # subfolder
    def index_data_source(self, data_source_path: str) -> None:
        with open(data_source_path, 
                  mode='rt', encoding='utf8', newline='') as data_file:
            data_file_name = path.splitext(path.basename(data_source_path))[0]
            data_reader = reader(data_file, delimiter='\t')
            
            # skip the first line (the header)
            data_file.readline()
            
            for doc in data_reader:
                
                # condition to dump index block to disk
                if len(self.memory_inverted_index.keys()) > 3:
                    # TODO: between runs of the same Indexer instance the 
                    # statistics will accumulate, this will need to be fixed
                    self.statistics['Number of temporary index segments'] += 1
                    
                    block_file_name = '{}.block{:000}.tsv'.format(
                        data_file_name, 
                        self.statistics['Number of temporary index segments'])
                    block_file_path = path.join('index', block_file_name)
                    
                    with open(block_file_path, mode='wt', encoding='utf8', 
                              newline='') as block_file:
                        block_writer = writer(block_file, delimiter='\t')
                        block_writer.writerow('a')
                    # for block_term in self.memory_inverted_index:
                    #     # text_row = ':'.join(block_term.keys())
                    #     # block_writer.writerow(':'.join(block_term))
                    #     block_writer.writerow(['a','b'])
                
                parsed_doc = self.parse_doc(doc)
                tokens = self.tokenizer.tokenize(parsed_doc[1])
                self.index_doc(parsed_doc[0], tokens)
        
    # fields other than reviewid are concatenated separated by spaces, as the
    # body of the document
    def parse_doc(self, doc: List[str]) -> List[str]:
        doc_id = doc[2]
        doc_body = '{} {} {}'.format(doc[5], doc[12], doc[13])
        
        return [doc_id, doc_body]

    def get_statistics(self) -> Dict[str, int]:
        return self.statistics
    
    def dump_index_to_disk(self) -> None:
        pass