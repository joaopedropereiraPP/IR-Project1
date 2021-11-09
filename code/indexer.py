import re
import sys

class Indexer:
    def __init__(self,initial_structure={}):
        self.indexed_words = initial_structure
        self.size_of_indexed_words = 0
    
    def get_indexed(self):
        return self.indexed_words

    def index(self,tokens):
        #print("###################INDEX INICIAL ########################")
        for token in tokens:

            term = token[0]
            idx = token[1]
            position = token[2]

            #print(token)
            #se o termo não existe nas palavras já indexadas
            if term not in self.indexed_words.keys():
                #adiciona nova palavra ao indexer
                posting = { idx : { 'weight' : 1 , 'positions' : [position] }}
                self.indexed_words[term] = { 'posting_list': posting, 'ndocs': 1, 'npositions': 1}
            else:
            #caso o termo já esteja indexado    
                new_doc = self.indexed_words[term]['posting_list']
                #Se o documento não está no termo
                if idx not in new_doc.keys():
                    new_doc[idx] = { 'weight' : 1 , 'positions' : [position] }
                    self.indexed_words[term]['ndocs'] += 1
                    self.indexed_words[term]['npositions'] += 1
                else:
                #Se o documento já está no termo
                    #soma 1 ao peso
                    new_doc[idx]['weight'] += 1
                    #adiciona a posição
                    new_doc[idx]['positions'].append(position)
                    self.indexed_words[term]['npositions'] += 1
                    #print("->"+term)
                #print(new_doc)
                self.indexed_words[term]['posting_list'] = new_doc

        self.size_of_indexed_words = len(self.indexed_words.keys())

