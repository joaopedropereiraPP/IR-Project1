from tokenizer import Tokenizer
import csv
from os import path
from collections import defaultdict
from bm25 import bm25
from time import time

class Query:
    doc_keys = {}
    doc_keys_folder_path: str
    master_index_folder_path: str
    master_index: dict
    tokenize: Tokenizer      


    def __init__(self, data_path , stopwords_path = '', stemmer_enabled = True, size_filter = 0, use_positions = False):
        
        #data
        self.data_path = data_path

        #path files
        self.doc_keys_folder_path = ('index/'  + path.basename(data_path).split('.')[0]  + '/DocKeys.tsv')
        self.master_index_folder_path = ('index/' + path.basename(data_path).split('.')[0] + '/MasterIndex.tsv')
        self.configurations_folder_path = ('index/' + path.basename(data_path).split('.')[0] + '/conf.tsv')


        self.doc_keys= (defaultdict(lambda: defaultdict(lambda: [])))
        self.master_index = defaultdict(lambda: defaultdict(dict))


        #configurations
        self.index_type = 'raw'
        self.size_filter = 0
        self.stemmer_enabled = True
        self.stopwords_path = ''
        self.use_positions = False

        #update configurations
        self.read_configurations()

        #initialize tokenizer
        self.tokenize = Tokenizer(stopwords_path = self.stopwords_path, 
                                stemmer_enabled = self.stemmer_enabled, 
                                size_filter = self.size_filter, 
                                use_positions=self.use_positions,
                                index_type=self.index_type)

        #update doc_keys
        self.read_doc_keys()

        #update master_index
        self.read_master_index()

    def process_query(self, term:str):

        print("Method used: {}".format(self.index_type))
        print("Words to search:")
        to_search = input()
        

        while ( to_search != '0'):
            
            #conjunto de palavras
            terms = self.term_tokenizer(to_search)
            start_time = time()
            if self.index_type == 'lnc.ltc':
                pass 
            elif self.index_type == 'bm25':
                bm25_method = bm25(data_path=self.data_path, doc_keys = self.doc_keys, master_index=self.master_index)
                bm25_method.bm25_search(terms)
            total_time = time() - start_time
            print("Time used: {:0.3f}s".format(total_time))
            print("Words to search:")
            to_search = input()



    def read_doc_keys(self):
        with open(self.doc_keys_folder_path, 'r') as file:
            filecontent = csv.reader(file, delimiter='\t')
            for row in filecontent:
                self.doc_keys[row[0]]['real_id'] = row[1]
                self.doc_keys[row[0]]['doc_name'] = row[2]
                self.doc_keys[row[0]]['doc_len'] = row[3]

    def read_master_index(self):
        with open(self.master_index_folder_path, 'r') as file:
            filecontent = csv.reader(file, delimiter='\t')
            for row in filecontent:
                term = row[0]
                number = row[1]
                file_path = row[2]
                self.master_index[term]['idf'] = number
                self.master_index[term]['file_path'] = file_path

    def read_configurations(self):
        with open(self.configurations_folder_path, 'r') as file:
            filecontent = list(csv.reader(file, delimiter='\t'))
            self.index_type = filecontent[0][1]
            self.size_filter = int(filecontent[1][1])
            self.stemmer_enabled = True if filecontent[2][1] == 'True' else False
            self.stopwords_path = filecontent[3][1]
            self.use_positions = True if filecontent[4][1] == 'True' else False

    def term_tokenizer(self, term):
        return self.tokenize.tokenize(input_string = term)