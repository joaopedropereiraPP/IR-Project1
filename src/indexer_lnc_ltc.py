from typing import List, Dict
from math import sqrt, log10

from indexer import Indexer
from postings import PostingWeighted, PostingWeightedPositional
from tokenizer import Tokenizer


class IndexerLncLtc(Indexer):
    logarithm: Dict[int, float]

    def __init__(self, tokenizer: Tokenizer,
                 max_postings_per_temp_block: int = 1000000,
                 use_positions=False) -> None:
        super().__init__(tokenizer, max_postings_per_temp_block,
                         use_positions=use_positions)
        self.logarithm = {}
        self.index_type = 'lnc.ltc'

    def create_postings(self, doc_id: int, doc_body: str) -> None:
        tokens = self.tokenizer.tokenize_positional(doc_body)
        Wtds = {}
        Wtdnorm = 0
        for token in tokens:
            tf = len(tokens[token])
            Wtds[token] = 1 + self.log(tf)
            Wtdnorm += (1 + self.log(tf)) ** 2
        Wtdnorm = sqrt(Wtdnorm)
        for token in Wtds:
            normalized_Wtd = Wtds[token] / Wtdnorm
            self.inverted_index[token].append(PostingWeighted(doc_id,
                                              normalized_Wtd))
            self.nr_postings += 1
            self.block_posting_count += 1

    def create_postings_positional(self, doc_id: int, doc_body: str) -> None:
        tokens = self.tokenizer.tokenize_positional(doc_body)
        Wtds = {}
        Wtdnorm = 0
        for token in tokens:
            tf = len(tokens[token])
            Wtds[token] = 1 + self.log(tf)
            Wtdnorm += (1 + self.log(tf)) ** 2
        Wtdnorm = sqrt(Wtdnorm)
        for token in Wtds:
            normalized_Wtd = Wtds[token] / Wtdnorm
            self.inverted_index[token].append(PostingWeightedPositional(doc_id,
                                              normalized_Wtd, tokens[token]))
            self.nr_postings += 1
            self.block_posting_count += 1

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

    def log(self, n):
        if n not in self.logarithm:
            self.logarithm[n] = log10(n)
        return self.logarithm[n]
