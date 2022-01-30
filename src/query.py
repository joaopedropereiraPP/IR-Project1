import csv
from collections import defaultdict
from configparser import ConfigParser
from math import log10, sqrt
from os import path
from time import time
from typing import Dict, List, DefaultDict

from postings import Posting, PostingPositional, PostingWeighted, \
                     PostingWeightedPositional
from tokenizer import Tokenizer


class Query:
    doc_keys = {}
    doc_keys_folder_path: str
    master_index_folder_path: str
    doc_keys: Dict[int, List[str]]
    master_index: Dict[str, List[float]]
    tokenizer: Tokenizer
    logarithm: Dict[int, float]
    files_to_open: DefaultDict[int, DefaultDict[str, int]]
    post_data: Dict[str, Dict[int, float]]

    def __init__(self, data_path, stopwords_path='', stemmer_enabled=True,
                 size_filter=0, use_positions=False, dump_results_file=True,
                 cmd_results=True):
        self.logarithm = {}
        self.search_text = ''
        self.dump_results_file = dump_results_file
        self.cmd_results = cmd_results

        # data
        self.data_path = data_path

        # path files
        # self.doc_keys_folder_path = data_path + '/DocKeys.tsv'
        # self.master_index_folder_path = data_path + '/MasterIndex.tsv'
        # self.posting_index_block_file = data_path + '/PostingIndexBlock{}.tsv'
        # self.configurations_folder_path = path.join(data_path, 'conf.ini')
        # self.query_result_file = data_path + '/query_result.txt'

        self.doc_keys_folder_path = path.join(data_path, 'DocKeys.tsv')
        self.master_index_folder_path = path.join(data_path, 'MasterIndex.tsv')
        self.posting_index_block_file = path.join(data_path, 'PostingIndexBlock{}.tsv')
        self.configurations_folder_path = path.join(data_path, 'conf.ini')
        self.query_result_file = path.join(data_path, 'query_result.txt')

        self.doc_keys = {}
        self.master_index = {}

        # configurations
        self.index_type = 'raw'
        self.size_filter = 0
        self.stemmer_enabled = True
        self.stopwords_path = ''
        self.use_positions = False

        # update configurations
        self.read_configurations()

        # initialize tokenizer
        self.tokenizer = Tokenizer(stopwords_path=self.stopwords_path,
                                   stemmer_enabled=self.stemmer_enabled,
                                   size_filter=self.size_filter)

        # update doc_keys
        self.read_doc_keys()

        # update master_index
        self.read_master_index()

        self.files_to_open = defaultdict(lambda: defaultdict(int))

        if self.dump_results_file:
            self.clean_query_results_file()

    def process_query(self, search_text):
        self.search_text = search_text
        terms = self.tokenizer.tokenize_positional(search_text)
        start_time = time()
        result = {}
        
        if self.index_type == 'lnc.ltc':
            result = self.lncltc_search(terms)
        elif self.index_type == 'bm25':
            result = self.bm25_search(terms)

        total_time = time() - start_time
        print('Time used: {:0.3f}s \n\n'.format(total_time))
        return result

    def read_doc_keys(self):
        with open(self.doc_keys_folder_path, 'r') as file:
            filecontent = csv.reader(file, delimiter='\t')
            for row in filecontent:
                self.doc_keys[int(row[0])] = row[1:3]

    def read_master_index(self):
        with open(self.master_index_folder_path, 'r') as file:
            filecontent = csv.reader(file, delimiter='\t')
            for row in filecontent:
                self.master_index[row[0]] = [float(value)
                                             for value in row[1:3]]

    def read_configurations(self):
        with open(self.configurations_folder_path, 'r') as file:
            filecontent = csv.reader(file, delimiter='\t')
            for row in filecontent:
                if row[0] == 'index_type':
                    self.index_type = row[1]
                elif row[0] == 'size_filter':
                    self.size_filter = int(row[1])
                elif row[0] == 'stemmer_enabled':
                    if row[1] == 'True':
                        self.stemmer_enabled = True
                    else:
                        self.stemmer_enabled = False
                elif row[0] == 'stopwords_path':
                    self.stopwords_path = row[1]
                elif row[0] == 'use_positions':
                    if row[1] == 'True':
                        self.use_positions = True
                    else:
                        self.use_positions = False

    def bm25_search(self, terms):
        self.post_data = {}

        bm25_ranking = defaultdict(float)
        self.files_to_open.clear()

        # Stores all documents with their terms to optimize the search within
        # the PostingIndexBlock file
        self.store_files_to_open(terms)

        for file_number in self.files_to_open:
            file_name = self.posting_index_block_file.format(int(file_number))
            self.read_posting_index_block(
                file_name, self.files_to_open[file_number])

            for term in self.files_to_open[file_number]:
                counter = self.files_to_open[file_number][term]
                for doc_id in self.post_data[term]:
                    bm25_ranking[doc_id] += self.post_data[term][doc_id] * counter

        results = []
        if not bm25_ranking:
            print('Nothing found!')
        for score, doc_id in sorted(((value, key) for (key,value) in bm25_ranking.items()), reverse=True):
            results.append(self.doc_keys[doc_id][0]
                           + ' -> ' + self.doc_keys[doc_id][1])

        if self.dump_results_file:
            self.dump_query_result(results)

        return results

    def lncltc_search(self, terms):
        self.post_data = {}

        lnc_ltc_ranking = defaultdict(float)
        self.files_to_open.clear()

        # Stores all documents with their terms to optimize the search within
        # the PostingIndexBlock file
        self.store_files_to_open(terms)

        Wtqs = {}
        Wtq_norm = 0
        common_terms = [term for term in terms if term in self.master_index]
        for term in common_terms:
            Wtq = (1 + self.log(len(terms[term]))) * self.master_index[term][0]
            Wtq_norm += Wtq ** 2
            Wtqs[term] = Wtq
        Wtq_norm = sqrt(Wtq_norm)

        for file_number in self.files_to_open:
            file_name = self.posting_index_block_file.format(file_number)
            self.read_posting_index_block(
                file_name, self.files_to_open[file_number])

            for term in self.files_to_open[file_number]:
                for doc_id in self.post_data[term]:
                    Wtq = self.post_data[term][doc_id]
                    lnc_ltc_ranking[doc_id] += Wtq * Wtqs[term] / Wtq_norm

        results = []
        if not lnc_ltc_ranking:
            print('Nothing found!')
        for score, doc_id in sorted(((value, key) for (key,value) in lnc_ltc_ranking.items()), reverse=True):
            results.append(self.doc_keys[doc_id][0]
                           + ' -> ' + self.doc_keys[doc_id][1])

        if self.dump_results_file:
            self.dump_query_result(results)

        return results


    def dump_query_result(self, results):
        with open(self.query_result_file, mode='a', encoding='utf8',
                  newline='') as f:
            f.write('Q: {} \n'.format(self.search_text))
            i = 0
            for result in results:
                if i < 100:
                    f.writelines(result)
                    f.write('\n')
                i += 1

    def store_files_to_open(self, terms):

        for term in terms:
            if term in self.master_index:
                doc = int(self.master_index[term][1])
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

    def clean_query_results_file(self):
        if path.exists(self.query_result_file):
            file = open(self.query_result_file, 'w')
            file.close()

    def log(self, n: int) -> float:
        if n not in self.logarithm:
            self.logarithm[n] = log10(n)
        return self.logarithm[n]
