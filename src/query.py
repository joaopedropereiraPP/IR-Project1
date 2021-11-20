import sys
from typing import Dict, List
from tokenizer import Tokenizer
from indexer import Indexer
import gzip
import csv
from sys import maxsize, argv, exit
from argparse import ArgumentParser
from os import path
import re
from collections import defaultdict


class Query:


    dock_keys= {}
    doc_keys_folder_path:str
    master_index_folder_path:str
    master_index:dict
    tokenize:Tokenizer      
    word_compressed:str  

    def __init__(self, doc_keys_folder_path = "index/doc_keys.tsv", 
                master_index_folder_path = "index/master_index.tsv", stopwords_path='',stemmer_enabled=True, size_filter=0, use_positions = False) :
        
        #DOC KEYS FILE
        # doc_id:pointer 
        self.dock_keys= {}
        self.doc_keys_folder_path = doc_keys_folder_path

        #MASTER INDEX FILE
        # { termo: { 'numbers': 5 , 'file_path': file1.tsv } , ... }
        self.master_index_folder_path = master_index_folder_path
        self.master_index = defaultdict(lambda: defaultdict(dict))

        self.tokenize = Tokenizer(stopwords_path='',stemmer_enabled=True, size_filter=0)      

        self.word_compressed = ''  

    def read_doc_keys(self):
        line = []
        with open(self.doc_keys_folder_path,'r')as file:
            filecontent= csv.reader(file)
            for row in filecontent:
                values =row[0].split(":")
                self.dock_keys[values[0]]=values[1]
        print(self.dock_keys)

    def read_master_index(self):
        line = []
        with open(self.master_index_folder_path,'r')as file:
            filecontent= csv.reader(file, delimiter='\t')
            for row in filecontent:
                term = row[0]
                number = row[1]
                file_path = row[2]
                self.master_index[term]['doc_freq'] = number
                self.master_index[term]['file_path'] = file_path

    def term_tokenizer(self, term):
        return self.tokenize.tokenize(input_string = term)
        
        

    def process_query(self, term:str):
        
        self.word_compressed = list(self.term_tokenizer(term))
        self.read_master_index()
        for word in self.word_compressed:
            print("Entered Word: " + term)
            print("Normalized word:" + word)
            print("Doc frequency: "+str("Not found" if self.master_index[word]['doc_freq']=={} else self.master_index[word]['doc_freq']))
            print("File to Search: "+str("Not found" if self.master_index[word]['file_path']=={} else self.master_index[word]['file_path']))