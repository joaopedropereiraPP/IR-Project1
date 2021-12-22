from typing import Dict, List
from math import log10

from indexer import Indexer
from postings import PostingWeighted, PostingWeightedPositional
from tokenizer import Tokenizer


class IndexerBM25(Indexer):
    k: float
    b: float
    avdl: float
    logarithm: Dict[int, float]

    def __init__(self, tokenizer: Tokenizer, k: float = 1.2, b: float = 0.75,
                 max_postings_per_temp_block: int = 1000000,
                 use_positions=False) -> None:
        super().__init__(tokenizer, max_postings_per_temp_block,
                         use_positions=use_positions)
        self.k = k
        self.b = b
        self.avdl = 0
        self.logarithm = {}
        self.index_type = 'bm25'

    def create_postings(self, doc_id: int, doc_body: str) -> None:
        tokens = self.tokenizer.tokenize_positional(doc_body)
        dl = 0
        for token in tokens:
            dl += len(tokens[token])
        self.doc_keys[doc_id].append(str(dl))
        for token in tokens:
            tf = len(tokens[token])
            self.inverted_index[token].append(PostingWeighted(doc_id, tf))
            self.nr_postings += 1
            self.block_posting_count += 1

    def create_postings_positional(self, doc_id: int, doc_body: str) -> None:
        tokens = self.tokenizer.tokenize_positional(doc_body)
        dl = 0
        for token in tokens:
            dl += len(tokens[token])
        self.doc_keys[doc_id].append(str(dl))
        for token in tokens:
            tf = len(tokens[token])
            self.inverted_index[token].append(
                PostingWeightedPositional(doc_id, tf, tokens[token]))
            self.nr_postings += 1
            self.block_posting_count += 1

    def final_indexing_calculations(self) -> None:
        self.update_dfs()
        self.calculate_avdl()
        self.calculate_weights()

    def calculate_avdl(self):
        sum_of_documents_length = 0
        ndocs = 0
        for docs in self.doc_keys.keys():
            sum_of_documents_length += int(self.doc_keys[docs][2])
            ndocs += 1
        self.avdl = sum_of_documents_length / ndocs

    def calculate_weights(self) -> None:
        for term in self.inverted_index:
            idf = self.master_index[term][0]
            for posting in self.inverted_index[term]:
                tf = posting.weight
                dl = int(self.doc_keys[posting.doc_id][2])
                dividend = (self.k + 1) * tf
                B = (1 - self.b) + self.b * (dl / self.avdl)
                divider = (self.k * B) + tf
                weight = dividend / divider
                posting.weight = weight * idf

    def posting_list_from_str(self,
                              posting_str_list: str) -> List[PostingWeighted]:
        if self.use_positions:
            posting_list = [PostingWeightedPositional.from_string(
                posting_str) for posting_str in posting_str_list]
        else:
            posting_list = [PostingWeighted.from_string(
                posting_str) for posting_str in posting_str_list]
        return posting_list

    def update_dfs(self):
        for term in self.master_index:
            df = self.master_index[term][0]
            idf = self.log(self.nr_indexed_docs) \
                - self.log(df)
            self.master_index[term][0] = idf

    def log(self, n: int) -> float:
        if n not in self.logarithm:
            self.logarithm[n] = log10(n)
        return self.logarithm[n]
