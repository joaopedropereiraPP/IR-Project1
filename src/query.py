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


class Query:
    master_index:Dict
    index_folder_path:str
    def __init__(self, index_folder_path = "index/master_index.tsv") :
        self.master_index = {}
        self.index_folder_path = index_folder_path

        
    def read_master_index(self):
        line = []
        with open(self.index_folder_path,'r')as file:
            filecontent= csv.reader(file)
            for row in filecontent:
                values = row[0].replace(" ", "")
                x =values.split(":")
                print(x[0])
                self.master_index[x[0]]=x[1]

        

    def process_query(self, term:str):
        pass