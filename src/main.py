import sys
from tokenizer import Tokenizer
from indexer import Indexer
from gzip import open
from csv import DictReader, field_size_limit
from sys import maxsize, argv, exit
from argparse import ArgumentParser
from os import path


class Main:
    def __init__(self) :


        #Default arguments
        self.data_path = ['content/amazon_reviews_us_Books_v1_00.tsv.gz' , 
                            'content/amazon_reviews_us_Digital_Music_Purchase_v1_00.tsv.gz', 
                            'content/amazon_reviews_us_Digital_Video_Games_v1_00.tsv.gz', 
                            'content/amazon_reviews_us_Music_v1_00.tsv.gz']
        self.use_stopwords = True
        self.stopwords_path = 'content/stopwords.txt'
        self.minimum_word_size = 3
        self.use_word_size = True
        self.stemmer_enabled = True

        self.parser = ArgumentParser()
        
        self.tokenizer = Tokenizer(stopwords_path = self.stopwords_path, stemmer_enabled = self.stemmer_enabled, size_filter=self.minimum_word_size)
        
        self.indexer = Indexer(tokenizer = self.tokenizer)
    

    def process(self, chunksize = 2) :
        """All the process start here"""
        maxInt = maxsize

        field_size_limit(maxInt)
        reviews=[]

        #original_file = "content/amazon_reviews_us_Digital_Video_Games_v1_00.tsv.gz"
        original_file = "../content/data.tsv.gz"
        
        print(self.data_path)
        for file in self.data_path:
            self.indexer.index_data_source(data_source_path = file)
         
 
    def parse_args(self):
        
        parser = ArgumentParser()
        #PATH TO NEW DATA FILE
        parser.add_argument("--data_path", help="Set the path to the data", 
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
        parser.add_argument("--nostemmer", help="Set not to use Stemmer",
                            action="store_false")

        return parser

    def check_arguments(self, parser, args):

        if args.data_path :
            self.data_path = [args.data_path]
            if not path.isfile(self.data_path[0]) or not self.data_path[0].endswith('.gz'):
                print("File does not exist or does not have the correct extension! ")
                print(parser.parse_args(['-h']))
                sys.exit()
        
        #SE NÃO QUER LISTA DE STOPWORS, MAS DEFINE UMA LISTA
        if (not args.nostopwords) and (args.stopwords != None):
            print(parser.parse_args(['-h']))
            sys.exit()

        if not args.nostopwords:
            self.use_stopwords = False
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
            self.use_word_size = False
            self.minimum_word_size = 0
        
        if not args.nostemmer:
            self.stemmer_enabled = False

    def main(self):

        #Create ad check all arguments
        parser = self.parse_args()
        args = parser.parse_args()
        self.check_arguments(parser, args)
        
        
        print("data_path: " + str(self.data_path))
        print("use_stopwords: " + str(self.use_stopwords))
        print("stopwords_path: " + str(self.stopwords_path))
        print("minimum_word_size: " + str(self.minimum_word_size))
        print("use_word_size: " + str(self.use_word_size))
        print("stemmer_enabled: " + str(self.stemmer_enabled))
        

        self.process()




if __name__ == "__main__":    
    Main().main()

    
