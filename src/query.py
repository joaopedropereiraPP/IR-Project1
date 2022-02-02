import csv
from collections import defaultdict
import difflib
from math import log2, log10, sqrt
from os import path
from statistics import mean, median
from time import perf_counter
from typing import DefaultDict, Dict, List, Tuple
from difflib import SequenceMatcher

from postings import PostingWeighted, PostingWeightedPositional
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
    positional_boost_enabled: bool
    span_size: int

    def __init__(self, data_path, stopwords_path='', stemmer_enabled=True,
                 size_filter=0, use_positions=False, dump_results_file=True,
                 cmd_results=True, positional_boost_enabled=True, span_size=4):
        self.logarithm = {}
        self.search_text = ''
        self.dump_results_file = dump_results_file
        self.cmd_results = cmd_results
        self.positional_boost_enabled = positional_boost_enabled
        self.span_size = span_size

        # data
        self.data_path = data_path

        # files
        self.doc_keys_folder_path = path.join(data_path, 'DocKeys.tsv')
        self.master_index_folder_path = path.join(data_path, 'MasterIndex.tsv')
        self.posting_index_block_file = path.join(data_path, 'PostingIndexBlock{}.tsv')
        self.configurations_folder_path = path.join(data_path, 'conf.ini')
        self.query_result_file = path.join(data_path, 'query_result.txt')
        self.evaluation_result_file = path.join(data_path, 'evaluation_query_result.txt')

        # memory indexes
        self.doc_keys = {}
        self.master_index = {}
        self.post_data = {}

        # configurations
        self.index_type = 'raw'
        self.size_filter = 0
        self.stemmer_enabled = True
        self.stopwords_path = ''
        self.use_positions = False

        # positional boosting tuning parameters
        self.max_boost_lncltc = 0.01
        self.max_boost_bm25 = 0.01
        self.query_similarity_factor = 3

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

    def process_query(self, search_text) -> Tuple[List[str], float]:
        self.search_text = search_text
        terms = self.tokenizer.tokenize_positional(search_text)
        start_time = perf_counter()
        result = {}
        
        if self.index_type == 'lnc.ltc':
            result = self.lncltc_search(terms)
        elif self.index_type == 'bm25':
            result = self.bm25_search(terms)

        total_time = perf_counter() - start_time
        return result, total_time

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
        bm25_ranking = defaultdict(float)
        self.files_to_open.clear()

        # Stores all documents with their terms to optimize the search within
        # the PostingIndexBlock file
        self.store_files_to_open(terms)

        posts = {}
        for file_number in self.files_to_open:
            file_name = self.posting_index_block_file.format(int(file_number))
            posts.update(self.post_data)
            
            self.read_posting_index_block(file_name, self.files_to_open[file_number])

            for term in self.files_to_open[file_number]:
                counter = self.files_to_open[file_number][term]
                for doc_id in self.post_data[term]:
                    bm25_ranking[doc_id] += self.post_data[term][doc_id][0] * counter

        # apply positional boost
        if self.use_positions and self.positional_boost_enabled:
            positions_index = defaultdict(list)
            query_term_list = []
            for term in terms:
                for term_position in terms[term]:
                    query_term_list.append(term)
            for term in posts:
                for doc_id in posts[term]:
                    for term_position in posts[term][doc_id][1]:
                        positions_index[doc_id].append((term_position, term))
            for doc_id in positions_index:
                positions_index[doc_id].sort(key=lambda x: x[0])
                bm25_ranking[doc_id] += self.calculate_positional_boost(query_term_list, positions_index[doc_id])

        results = []
        for score, doc_id in sorted(((value, key) for (key,value) in bm25_ranking.items()), reverse=True):
            results.append((self.doc_keys[doc_id][0], self.doc_keys[doc_id][1]))

        if self.dump_results_file:
            self.dump_query_result(results)

        return results

    def lncltc_search(self, terms):
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

        posts = {}
        for file_number in self.files_to_open:
            file_name = self.posting_index_block_file.format(file_number)
            posts.update(self.post_data)

            self.read_posting_index_block(file_name, self.files_to_open[file_number])

            for term in self.files_to_open[file_number]:
                for doc_id in self.post_data[term]:
                    Wtd = self.post_data[term][doc_id][0]
                    lnc_ltc_ranking[doc_id] += Wtd * Wtqs[term] / Wtq_norm

        # apply positional boost
        if self.use_positions and self.positional_boost_enabled:
            positions_index = defaultdict(list)
            query_term_list = []
            for term in terms:
                for term_position in terms[term]:
                    query_term_list.append(term)
            for term in posts:
                for doc_id in posts[term]:
                    for term_position in posts[term][doc_id][1]:
                        positions_index[doc_id].append((term_position, term))
            for doc_id in positions_index:
                positions_index[doc_id].sort(key=lambda x: x[0])
                lnc_ltc_ranking[doc_id] += self.calculate_positional_boost(query_term_list, positions_index[doc_id])

        results = []
        for score, doc_id in sorted(((value, key) for (key,value) in lnc_ltc_ranking.items()), reverse=True):
            results.append((self.doc_keys[doc_id][0], self.doc_keys[doc_id][1]))

        if self.dump_results_file:
            self.dump_query_result(results)

        return results

    def calculate_positional_boost(self, query_term_list: List[str], positions_list: List[Tuple[int, str]]) -> float:
        if self.index_type == 'lnc.ltc':
            max_boost = self.max_boost_lncltc
        elif self.index_type == 'bm25':
            max_boost = self.max_boost_bm25

        total_boost = 0
        i = 0
        while i + 1 < len(positions_list):
            contained_terms = []
            contained_terms.append(positions_list[i])

            # consider all terms after the current one that are within the span
            start_position = positions_list[i][0]
            i += 1
            while i < len(positions_list) and positions_list[i][0] - start_position <= self.span_size:
                contained_terms.append(positions_list[i])
                i += 1

            # consider all terms after the last one within the span that fall within the span centered on that last one
            start_position = positions_list[i - 1][0]
            # i += 1
            # print(positions_list[i][0] - start_position)
            while i < len(positions_list) and positions_list[i][0] - start_position <= self.span_size:
                contained_terms.append(positions_list[i])
                i += 1

            span = contained_terms[-1][0] - contained_terms[0][0]
            # print('terms:', contained_terms, 'nr_terms:', len(contained_terms), 'span:', span)
            if len(contained_terms) > 0 and span > 0:
                query_similarity = SequenceMatcher(None, query_term_list, [term[1] for term in contained_terms]).ratio() * self.query_similarity_factor
                total_boost += len(contained_terms) / span * query_similarity * max_boost

        return total_boost

    def dump_query_result(self, results):
        with open(self.query_result_file, mode='a', encoding='utf8',
                  newline='') as f:
            f.write('Q: {} \n'.format(self.search_text))
            i = 0
            for result in results:
                if i < 100:
                    f.writelines(result[0])
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
                            post[posting.doc_id] = (posting.weight, posting.positions)
                        else:
                            posting = PostingWeighted.from_string(content[n])
                            post[posting.doc_id] = (posting.weight,)
                    self.post_data[term] = post

    def clean_query_results_file(self):
        if path.exists(self.query_result_file):
            file = open(self.query_result_file, 'w')
            file.close()

    def log(self, n: int) -> float:
        if n not in self.logarithm:
            self.logarithm[n] = log10(n)
        return self.logarithm[n]

    def read_silver_standard_file(self, file_path: str) -> Dict[str, List[Tuple[str, int]]]:
        """
        reads the file with the silver standard results and returns a dict containing: {'queryText': [(doc1, rel), (doc2, rel), ...]}, where rel is the doc's relevance value from 1 to 3
        """
        silver_standard_results = {}
        with open(file_path, 'r') as file:
            lines = file.read().split("\n")
            for line in lines:
                # detect query terms
                if line[:2] == "Q:":
                    query_str = line[2:]
                    silver_standard_results[query_str] = []
                elif line != "": # if the line is not empty line
                    doc_id = line.split("\t")[0]
                    doc_relevance = int(line.split("\t")[1])
                    silver_standard_results[query_str].append((doc_id, doc_relevance))
        return silver_standard_results

    def evaluate_query_results(self, query_str, standard_results: List[Tuple[str, int]]) -> Dict[int, Dict[str, float]]:
        """
        evaluates the results for a single query.
        It receives as a first argument the query string and as a second argument the silver standard results as returned by read_silver_standard_file(), and then returns a dict of dicts containing IR evalutation statistics considering the top 10, 20 and 50 docs in the ranking with the following structure:
        {10: {'stat1': value, 'stat2': value, ..., 'time': value},
         20: {'stat1': value, 'stat2': value, ..., 'time': value},
         50: {'stat1': value, 'stat2': value, ..., 'time': value}}
        """
        # repeat the query 10 times to stabilize time metrics
        query_times = []
        for i in range(1):
            query_ranking, query_time = self.process_query(query_str)
            query_times.append(query_time)
        mean_query_time = mean(query_times)

        standard_results_set = {doc_id for doc_id, doc_relevance in standard_results}
        standard_results_list = [doc_id for doc_id, doc_relevance in standard_results]

        query_evaluation = defaultdict(lambda: defaultdict(float))
        for rank_nr in [10, 20, 50]:

            dcg_norm = standard_results[0][1]
            for rank in range(2, min(rank_nr + 1, len(standard_results) + 1)):
                dcg_norm += standard_results[rank - 1][1] / log2(rank)

            dcg = standard_results[standard_results_list.index(query_ranking[0][0])][1] if query_ranking[0][0] in standard_results_list else 0

            avg_precision = 0
            for rank in range(1, min(rank_nr + 1, len(query_ranking) + 1)):
                query_results_set = {doc_id for doc_id, doc_title in query_ranking[:rank]}

                if query_ranking[rank - 1][0] in standard_results_set:
                    tp = len(query_results_set & standard_results_set)
                    fp = len(query_results_set - standard_results_set)
                    precision = tp / (tp + fp)
                    avg_precision += precision
                    if rank > 1:
                        dcg += standard_results[standard_results_list.index(query_ranking[rank - 1][0])][1] / log2(rank)

            avg_precision = avg_precision / len(standard_results)
            tp = len(query_results_set & standard_results_set)
            fp = len(query_results_set - standard_results_set)
            fn = len(standard_results_set - query_results_set)
            precision = tp / (tp + fp)
            recall = tp / (tp + fn)
            if tp > 0:
                fmeasure = (2 * recall * precision)/(recall + precision)
            else:
                fmeasure = 0

            dic_value = {
                'Precision': precision,
                'Recall': recall,
                'F-measure':fmeasure,
                'Average precision': avg_precision,
                'NDCG': dcg / dcg_norm,
                'time': mean_query_time,
            }

            query_evaluation[rank_nr] = dic_value

        return query_evaluation

    def evaluate_mean_statistics(self, query_statistics: List[Dict[int, Dict[str, float]]]) -> Dict[int, Dict[str, float]]:
        """
        receives a list of dicts as defined in evaluate_query_results() (one entry of the list for each query) and returns the average for each stat and the median latency and query throughput with the form:
        {10: {'stat1': value, 'stat2': value, ..., 'Query throughput': value, 'Median query latency'},
         20: {'stat1': value, 'stat2': value, ..., 'Query throughput': value, 'Median query latency'},
         50: {'stat1': value, 'stat2': value, ..., 'Query throughput': value, 'Median query latency'},
        """
        # aggregate query statistics in lists
        mean_statistics = {}
        for top_nr in query_statistics[0]:
            mean_statistics[top_nr] = {}
            for stat in query_statistics[0][top_nr]:
                mean_statistics[top_nr][stat] = []
                for query in query_statistics:
                    mean_statistics[top_nr][stat].append(query[top_nr][stat])

        # calculate the mean for all statistics except time
        for top_nr in mean_statistics:
            for stat in [st for st in  mean_statistics[top_nr] if st != 'time']:
                mean_statistics[top_nr][stat] = mean(mean_statistics[top_nr][stat])

        # calculate statistics that depend on time
        for top_nr in mean_statistics:
            mean_statistics[top_nr]['Average query throughput (queries/s)'] = 1 / mean(mean_statistics[top_nr]['time'])
            mean_statistics[top_nr]['Median query latency (ms)'] = median(mean_statistics[top_nr]['time']) * 1000
            del mean_statistics[top_nr]['time']

        return mean_statistics

    def evaluate_system(self, file_path: str) -> None:
        """
        the main method to evaluate the system
        """
        standard_results = self.read_silver_standard_file(file_path)

        query_statistics = []   # a list with the statistics for each query
        for query in standard_results:
            query_statistics.append(self.evaluate_query_results(query, standard_results[query]))

        return self.evaluate_mean_statistics(query_statistics)

    def show_evaluation_results(self, result):
        for rank in result.keys():
            print("\nTOP {}".format(rank))
            for stat in result[rank].keys():
                print("{}: {:.2f}".format(stat, result[rank][stat]))

    def dump_evaluation_result(self, result):
        print(self.evaluation_result_file)
        with open(self.evaluation_result_file, mode='w', encoding='utf8', newline='') as f:
            for rank in result.keys():
                f.writelines("TOP {} \n".format(rank))
                for stat in result[rank].keys():
                    f.writelines("{}: {:.2f}\n".format(stat, result[rank][stat]))
                f.write('\n')