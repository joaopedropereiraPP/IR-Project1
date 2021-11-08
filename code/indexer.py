import re
import os
import psutil
import sys
import math
import ast

class Indexer:
    def __init__(self,initial_structure={}):
        self.indexed_words = initial_structure
    
    
    def get_indexed(self):
        return self.indexed_words

    def index(self,tokens):
        print("###################INDEX INICIAL ########################")
        for token in tokens:

            term = token[0]
            idx = token[1]
            position = token[2]

            print(token)
            #se o termo não existe nas palavras já indexadas
            if term not in self.indexed_words.keys():
                #adiciona nova palavra ao indexer
                posting = { idx : { 'weight' : 1 , 'positions' : [position] }}
                self.indexed_words[term] = { 'doc_ids': posting, 'idf': None, 'doc_freq': 1, 'col_freq': 1}
            else:
            #caso o termo já esteja indexado    
                new_doc = self.indexed_words[term]['doc_ids']
                #Se o documento não está no termo
                if idx not in new_doc.keys():
                    new_doc[idx] = { 'weight' : 1 , 'positions' : [position] }
                    self.indexed_words[term]['doc_freq'] += 1
                    self.indexed_words[term]['col_freq'] += 1
                else:
                #Se o documento já está no termo
                    #soma 1 ao peso
                    new_doc[idx]['weight'] += 1
                    #adiciona a posição
                    new_doc[idx]['positions'].append(position)
                    self.indexed_words[term]['col_freq'] += 1
                    print("->"+term)
                print(new_doc)
                self.indexed_words[term]['doc_ids'] = new_doc

  
        print(str(len(self.indexed_words.keys())))
        print("###################INDEX FIM ########################")

        #print(str(len(self.indexed_words)))

