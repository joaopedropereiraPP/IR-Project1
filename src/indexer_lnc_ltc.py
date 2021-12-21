from typing import List, Dict
from math import sqrt, log10

from indexer import Indexer
from postings import PostingWeighted, PostingWeightedPositional
from tokenizer import Tokenizer


class IndexerLncLtc(Indexer):
    logarithm: Dict[int, float]

    def __init__(self, tokenizer: Tokenizer, k: float = 1.2, b: float = 0.75,
                 max_postings_per_temp_block: int = 1000000,
                 index_type: str = 'raw', use_positions=False) -> None:
        super().__init__(tokenizer, max_postings_per_temp_block,
                         use_positions=use_positions)
        self.logarithm = {}
        self.index_type = 'lnc.ltc'

    def create_postings(self, doc_id: int, doc_body: str) -> None:
        tokens = self.tokenizer.tokenize_positional(doc_body)
        for token in tokens:
            tf = len(tokens[token])
            Wtd = 1 + self.log(tf)
            self.inverted_index[token].append(PostingWeighted(doc_id, Wtd))
            self.nr_postings += 1
            self.block_posting_count += 1

    def create_postings_positional(self, doc_id: int, doc_body: str) -> None:
        tokens = self.tokenizer.tokenize_positional(doc_body)
        for token in tokens:
            tf = len(tokens[token])
            Wtd = 1 + self.log(tf)
            self.inverted_index[token].append(
                PostingWeightedPositional(doc_id, Wtd, tokens[token]))
            self.nr_postings += 1
            self.block_posting_count += 1

    def final_indexing_calculations(self) -> None:
        self.calculate_weights()

    def calculate_weights(self) -> None:
        for term in self.inverted_index:
            Wtdnorm = 0
            for posting in self.inverted_index[term]:
                Wtdnorm += posting.weight * posting.weight
            for posting in self.inverted_index[term]:
                normalized_weight = sqrt(Wtdnorm)
                posting.weight = posting.weight / normalized_weight

    def posting_list_from_str(self,
                              posting_str_list: str) -> List[PostingWeighted]:
        if self.use_positions:
            posting_list = [PostingWeightedPositional.from_string(
                posting_str) for posting_str in posting_str_list]
        else:
            posting_list = [PostingWeighted.from_string(
                posting_str) for posting_str in posting_str_list]
        return posting_list

    def calculate_df(self, term: str) -> float:
        idf = self.log(self.nr_indexed_docs) \
            - self.log(self.master_index[term][0])
        return idf

    def log(self, n):
        if n not in self.logarithm:
            self.logarithm[n] = log10(n)
        return self.logarithm[n]
