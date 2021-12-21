from tokenizer import Tokenizer
import csv
from os import path
from collections import defaultdict
from bm25 import bm25
from time import time
from configparser import ConfigParser

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
        self.posting_index_block_file = 'index/'+ path.basename(data_path).split('.')[0]+'/PostingIndexBlock{}.tsv'
        self.configurations_folder_path = ('index/' + path.basename(data_path).split('.')[0] + '/conf.ini')


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
                                size_filter = self.size_filter)

        #update doc_keys
        self.read_doc_keys()

        #update master_index
        self.read_master_index()

        self.files_to_open = {}

        
    def process_query(self, to_search):
        #conjunto de palavras
        terms = self.term_tokenizer(to_search)
        start_time = time()
        if self.index_type == 'lnc.ltc':
            pass 

        elif self.index_type == 'bm25':

            self.bm25_search(terms)

        total_time = time() - start_time
        print("Time used: {:0.3f}s".format(total_time))




    def read_doc_keys(self):
        with open(self.doc_keys_folder_path, 'r') as file:
            filecontent = csv.reader(file, delimiter='\t')
            for row in filecontent:
                self.doc_keys[row[0]]['real_id'] = row[1]
                self.doc_keys[row[0]]['doc_name'] = row[2]
                #self.doc_keys[row[0]]['doc_len'] = row[3]

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
        config = ConfigParser()
        config.read(self.configurations_folder_path)
        self.index_type = config['METADATA']['index_type']
        self.size_filter = int( config['METADATA']['size_filter'] )
        self.stemmer_enabled = True if config['METADATA']['stemmer_enabled'] == 'True' else False
        self.stopwords_path = config['METADATA']['stopwords_path']
        self.use_positions = True if config['METADATA']['use_positions'] == 'True' else False

    def term_tokenizer(self, term):
        if self.use_positions:
            return self.tokenize.tokenize_positional(input_string = term)
        else:
            return self.tokenize.tokenize(input_string = term)



###  BEST MATCH 25
    def bm25_search(self, terms):
        self.post_data = {}
        
        bm25_ranking = {}

        #Stores all documents with their terms to optimize the search within the PostingIndexBlock file 
        self.store_files_to_open(terms)

        for file_number in self.files_to_open.keys():
            
            file_name = self.posting_index_block_file.format(file_number)
            self.read_posting_index_block(file_name)

            index = 0
            for terms_on_file in self.files_to_open[file_number]:

                for any_term in terms_on_file.keys():
                    for docs in self.post_data[any_term].keys():
                        if docs not in bm25_ranking:
                            bm25_ranking[docs] = 0
                            idf = float(self.files_to_open[file_number][index][any_term]['idf'])
                            weight = float(self.post_data[any_term][docs])
                            count = int(self.files_to_open[file_number][index][any_term]['count'])
                        bm25_ranking[docs] += ((weight * idf) * count)
                    index += 1
        bm25_ranking = sorted(bm25_ranking.items(), key=lambda x: x[1], reverse=True) 
        for i in range(0,10):
            if len(bm25_ranking) >= i + 1:
                doc=self.doc_keys[tuple(list(bm25_ranking)[i])[0]]['real_id']
                doc_name=self.doc_keys[tuple(list(bm25_ranking)[i])[0]]['doc_name']
                print("{}º {} -> {}".format(i, doc, doc_name))



    def store_files_to_open(self, terms):
        term_result={}
        result={}
        for term in terms.keys():
            term_size = len(terms[term])
            doc = self.master_index[term]['file_path']
            idf = self.master_index[term]['idf']

            term_result={}
            result={}
            result['idf'] = idf
            result['count'] = term_size
            
            term_result[term] = result
            if doc not in self.files_to_open:
                self.files_to_open[doc] = []

            self.files_to_open[doc].append(term_result)

    def read_posting_index_block(self, file_to_analyse):
        self.post_data = {}
        with open(file_to_analyse, 'r') as file:
                    filecontent = csv.reader(file, delimiter='\t')
                    for a in filecontent:
                        term = a[0]
                        post = {}
                        for n in range(1, len(a)):
                            values = a[n].split(":")
                            doc_id = values[0]
                            weight = values[1]
                            post[doc_id] = weight
                        self.post_data[term] = post