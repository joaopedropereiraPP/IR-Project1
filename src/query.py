import csv
from collections import defaultdict
from configparser import ConfigParser
from os import path
from time import time

from bm25 import bm25
from postings import (Posting, PostingPositional, PostingWeighted,
                      PostingWeightedPositional)
from tokenizer import Tokenizer


class Query:
    doc_keys = {}
    doc_keys_folder_path: str
    master_index_folder_path: str
    master_index: dict
    tokenize: Tokenizer

    def __init__(self, data_path, stopwords_path='', stemmer_enabled=True,
                 size_filter=0, use_positions=False, dump_results_file=True,
                 cmd_results=True):

        self.to_search = ''
        self.dump_results_file = dump_results_file
        self.cmd_results = cmd_results

        # data
        self.data_path = data_path

        # path files
        self.doc_keys_folder_path = (
            'index/' + path.basename(data_path).split('.')[0] + '/DocKeys.tsv')
        self.master_index_folder_path = (
            'index/' + path.basename(data_path).split('.')[0] + '/MasterIndex.tsv')
        self.posting_index_block_file = 'index/' + \
            path.basename(data_path).split('.')[0]+'/PostingIndexBlock{}.tsv'
        self.configurations_folder_path = (
            'index/' + path.basename(data_path).split('.')[0] + '/conf.ini')
        self.query_result_file = 'index/' + \
            path.basename(data_path).split('.')[0]+'/query_result.txt'

        self.doc_keys = (defaultdict(lambda: defaultdict(lambda: [])))
        self.master_index = defaultdict(lambda: defaultdict(dict))

        # configurations
        self.index_type = 'raw'
        self.size_filter = 0
        self.stemmer_enabled = True
        self.stopwords_path = ''
        self.use_positions = False

        # update configurations
        self.read_configurations()

        # initialize tokenizer
        self.tokenize = Tokenizer(stopwords_path=self.stopwords_path,
                                  stemmer_enabled=self.stemmer_enabled,
                                  size_filter=self.size_filter)

        # update doc_keys
        self.read_doc_keys()

        # update master_index
        self.read_master_index()

        self.files_to_open = defaultdict(lambda: defaultdict(int))

    def process_query(self, to_search):
        self.to_search = to_search
        # conjunto de palavras
        terms = self.term_tokenizer(to_search)
        start_time = time()
        print('Q: {}'.format(to_search.replace('\n', '')))
        if self.index_type == 'lnc.ltc':
            pass

        elif self.index_type == 'bm25':

            self.bm25_search(terms)

        total_time = time() - start_time
        print('Time used: {:0.3f}s \n\n'.format(total_time))

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
        self.size_filter = int(config['METADATA']['size_filter'])
        self.stemmer_enabled = \
            True if config['METADATA']['stemmer_enabled'] == 'True' else False
        self.stopwords_path = config['METADATA']['stopwords_path']
        self.use_positions = \
            True if config['METADATA']['use_positions'] == 'True' else False

    def term_tokenizer(self, term):
        if self.use_positions:
            return self.tokenize.tokenize_positional(input_string=term)
        else:
            return self.tokenize.tokenize(input_string=term)

    # BEST MATCH 25
    def bm25_search(self, terms):
        self.post_data = {}

        bm25_ranking = defaultdict(int)
        self.files_to_open.clear()

        # Stores all documents with their terms to optimize the search within
        # the PostingIndexBlock file
        self.store_files_to_open(terms)

        for file_number in self.files_to_open.keys():
            file_name = self.posting_index_block_file.format(file_number)
            self.read_posting_index_block(
                file_name, self.files_to_open[file_number])

            for i in self.files_to_open[file_number]:
                counter = self.files_to_open[file_number][i]
                for post in self.post_data[i].keys():
                    bm25_ranking[post] += self.post_data[i][post] * counter

        results = []
        for i in sorted(bm25_ranking):
            results.append(self.doc_keys[str(i)]['real_id']
                           + ' -> ' + self.doc_keys[str(i)]['doc_name'])

        if self.dump_results_file:
            self.dump_query_result(results)

        if self.cmd_results:
            i = 0
            #print('Q: {}'.format(self.to_search.replace('\n','')))
            for result in results:
                if i < 10:
                    print(result)
                i += 1

    def dump_query_result(self, results):
        with open(self.query_result_file, mode='a', encoding='utf8',
                  newline='') as f:
            f.write('Q: {} \n'.format(self.to_search))
            i = 0
            for result in results:
                if i < 100:
                    f.writelines(result)
                    f.write('\n')
                i += 1

    def store_files_to_open(self, terms):

        for term in terms.keys():
            doc = self.master_index[term]['file_path']
            self.files_to_open[doc][term] = len(terms[term])

    def read_posting_index_block(self, file_to_analyse, terms_to_analyse):
        self.post_data.clear()
        with open(file_to_analyse, 'r') as file:
            filecontent = csv.reader(file, delimiter='\t')
            for content in filecontent:
                term = content[0]

                if term in terms_to_analyse:

                    post = {}
                    for n in range(1, len(content)):
                        if self.use_positions:
                            posting = PostingWeightedPositional.from_string(
                                content[n])
                            post[posting.doc_id] = posting.weight
                        else:
                            posting = PostingWeighted.from_string(content[n])
                            post[posting.doc_id] = posting.weight
                    self.post_data[term] = post
