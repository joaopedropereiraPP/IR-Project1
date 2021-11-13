import re
import sys

class Indexer:
    def __init__(self,initial_structure={}):
        self.indexed_words = initial_structure
        self.size_of_indexed_words = 0
    
    def get_indexed(self):
        return self.indexed_words

    def index(self,tokens):
     
        for token in tokens:

            term = token[0]
            idx = token[1]
            position = token[2]

           
            if term not in self.indexed_words.keys():
           
                posting = { idx : { 'weight' : 1 , 'positions' : [position] }}
                self.indexed_words[term] = { 'posting_list': posting, 'documents_frequency': 1}
            else:
           
                new_doc = self.indexed_words[term]['posting_list']
              
                if idx not in new_doc.keys():
                    new_doc[idx] = { 'weight' : 1 , 'positions' : [position] }
                    self.indexed_words[term]['documents_frequency'] += 1
          
                else:
          
                    new_doc[idx]['weight'] += 1
                    new_doc[idx]['positions'].append(position)
                   
                
                self.indexed_words[term]['posting_list'] = new_doc

        self.size_of_indexed_words = len(self.indexed_words.keys())

