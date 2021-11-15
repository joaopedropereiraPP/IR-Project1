from collections import defaultdict

class Indexer:
    def __init__(self,initial_structure={}):
        self.memory_inverted_index = defaultdict(lambda: 
                                                 defaultdict(lambda: []))
        self.memory_document_frequency = defaultdict(int)
    
    def get_indexed(self):
        return self.memory_inverted_index

    def index(self, doc_id, tokens):
        
        for token in tokens:
            
            self.memory_inverted_index[token][doc_id] = tokens[token]
            self.memory_document_frequency[token] += 1
