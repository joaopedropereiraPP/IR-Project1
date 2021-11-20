import sys
from typing import List
from tokenizer import Tokenizer
from indexer import Indexer
from query import Query
from gzip import open
from csv import DictReader, field_size_limit
from sys import maxsize, argv, exit
from argparse import ArgumentParser
from os import path


class Main:
    data_path:str
    stopwords_path:str
    minimum_word_size:int
    stemmer_enabled:bool
    use_positions:bool
    parser:ArgumentParser
    tokenizer:Tokenizer     
    indexer:Indexer

    def __init__(self) :


        #Default arguments
        self.data_path = ""

        self.stopwords_path = 'content/stopwords.txt'
        self.minimum_word_size = 3
        self.stemmer_enabled = True
        self.use_positions = False
        self.max_post = 1000000
        self.parser = ArgumentParser()
        self.tokenizer = Tokenizer(stopwords_path = self.stopwords_path, 
                                stemmer_enabled = self.stemmer_enabled, 
                                size_filter=self.minimum_word_size, 
                                use_positions=self.use_positions)
        
        
        self.indexer = Indexer(tokenizer = self.tokenizer, max_postings_per_temp_block = self.max_post)



        
         
 
    def parse_args(self):
        
        parser = ArgumentParser()
        #PATH TO NEW DATA FILE
        parser.add_argument("--data_path", help="Set the path to the data", required=True, 
                            type=str, metavar="(path to data file (.gz))")
        #NOT USE STOPWORDS LIST
        parser.add_argument("--nostopwords", help="Set not to use Stop Words List",
                            action="store_false")
        #PATH TO NEW STOPWORDS
        parser.add_argument("--stopwords", help="Set the path to Stop Words List", 
                            type=str, metavar="(path to stopwords list)")
        #MINIMUM WORD SIZE
        parser.add_argument("--word_size", help="Set the minimum words size", 
                            type=int, metavar="(integer number)")
        #NO MINIMUM WORD SIZE
        parser.add_argument("--no_word_size", help="Set not to use minimum words size",
                            action="store_false")
        #NOT USE STEMMER APPROACH
        parser.add_argument("--no_stemmer", help="Set not to use Stemmer",
                            action="store_false")
        #NOT USE POSITIONS
        parser.add_argument("--use_positions", help="Set to use positions",
                            action="store_true")

        parser.add_argument("--max_post", help="Set the maximum postings per temp block",
                            type=int)
        return parser

    def check_arguments(self, parser, args):

        if args.data_path :
            self.data_path = args.data_path
            if not path.exists(self.data_path) or not self.data_path.endswith('.gz'):
                print("File does not exist or does not have the correct extension! ")
                print(parser.parse_args(['-h']))
                sys.exit()
        
        #SE NÃO QUER LISTA DE STOPWORDS, MAS DEFINE UMA LISTA
        if (not args.nostopwords) and (args.stopwords != None):
            print(parser.parse_args(['-h']))
            sys.exit()

        if not args.nostopwords:
            self.stopwords_path = ''

        if args.stopwords:
            self.stopwords_path = args.stopwords

        #SE NÃO QUER TAMANHO MINIMO DE LETRAS, MAS DEFINE UM TAMANHO
        if (not args.no_word_size) and (args.word_size != None):
            print(parser.parse_args(['-h']))
            sys.exit()
        
        if args.word_size:
            self.minimum_word_size = args.word_size
 
        if not args.no_word_size:
            self.minimum_word_size = 0
        
        if not args.no_stemmer:
            self.stemmer_enabled = False

        if not args.use_positions:
            self.use_positions = True 

        if args.max_post:
            self.max_post = args.max_post     

    def main(self):

        #Create ad check all arguments
        parser = self.parse_args()
        args = parser.parse_args()
        self.check_arguments(parser, args)

        self.indexer.index_data_source(data_source_path = self.data_path)

        statistics = self.indexer.get_statistics()


        for i in statistics.keys():
            print(i + ": " + str(statistics[i]))



        query = Query(stopwords_path = self.stopwords_path, 
                        stemmer_enabled = self.stemmer_enabled, 
                        size_filter=self.minimum_word_size, 
                        use_positions=self.use_positions,
                        data_path=self.data_path)


        print("Word to Search:")
        word_to_search = input()
        while word_to_search != '0':
            
            query.process_query(word_to_search)
            print("Word to Search ( 0 to exit ):")
            word_to_search = input()


if __name__ == "__main__":    
    
    Main().main()

    
