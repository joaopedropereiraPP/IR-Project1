from tokenizer import Tokenizer
from collections import defaultdict
from typing import List, Dict
from gzip import open
from csv import reader

class Indexer:
    memory_inverted_index: Dict[str, Dict[str, List[int]]]
    memory_document_frequency: Dict[str, int]
    tokenizer: Tokenizer
    
    def __init__(self, tokenizer: Tokenizer) -> None:
        self.tokenizer = tokenizer
        self.memory_inverted_index = defaultdict(lambda: 
                                                 defaultdict(lambda: []))
        self.memory_document_frequency = defaultdict(int)
    
    def get_memory_inverted_index(self) -> Dict[str, Dict[str, List[int]]]:
        return self.memory_inverted_index

    def index_doc(self, doc_id, tokens):
        for token in tokens:
            
            self.memory_inverted_index[token][doc_id] = tokens[token]
            self.memory_document_frequency[token] += 1

    def index_data_source(self, data_source_path: str) -> None:
        data_file = open(data_source_path, 
                         mode='rt', encoding='utf8', newline='')
        data_reader = reader(data_file, delimiter='\t')
        
        #skip the first line (the header)
        data_file.readline()
        
        for doc in data_reader:
            parsed_doc = self.parse_doc(doc)
            doc_id = parsed_doc[0]
            word_list = parsed_doc[1]
            tokens = self.tokenizer.tokenize(word_list)
            self.index_doc(doc_id, tokens)
        
    # fields other than reviewid are concatenated separated by spaces, as the
    # body of the document
    def parse_doc(self, doc: List[str]) -> List[str]:
        doc_id = doc[2]
        doc_body = '{} {} {}'.format(doc[5], doc[12], doc[13])
        
        return [doc_id, doc_body]
