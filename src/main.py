import sys
from tokenizer import Tokenizer
from indexer import Indexer
from query import Query
from argparse import ArgumentParser
from os import path
from time import time
from indexer_bm25 import IndexerBM25

class Main:
    data_path: str
    stopwords_path: str
    minimum_word_size: int
    stemmer_enabled: bool
    use_positions: bool
    parser: ArgumentParser
    tokenizer: Tokenizer     
    indexer: Indexer

    def __init__(self):

        # Main Mode
        self.mode = ''

        # Indexer mode
        self.index_type = ''
        self.data_path = ""
        self.stopwords_path = 'content/stopwords.txt'
        self.minimum_word_size = 3
        self.stemmer_enabled = True
        self.use_positions = False
        self.max_post = 1000000
        
        #searcher mode
        self.data = ''
        self.search_type = ''
        self.loop = False
        self.query_file = ''

    
    
        self.parser = ArgumentParser()
        self.tokenizer = Tokenizer(stopwords_path = self.stopwords_path, 
                                stemmer_enabled = self.stemmer_enabled, 
                                size_filter = self.minimum_word_size)
        self.indexer = Indexer(tokenizer = self.tokenizer, max_postings_per_temp_block = self.max_post,
            use_positions=self.use_positions)
        
        
    def parse_args(self):
        parser = ArgumentParser()
        # Set the mode
        parser.add_argument("--mode", help="Set the main mode", required=True, 
                            type=str, metavar="indexer/searcher")
        
        ##IF IS INDEXER MODE
        #set method
        parser.add_argument("--method", help="Set the method", 
                            type=str, metavar="raw/lnc.ltc/bm25")
        # path to new data file
        parser.add_argument("--data_path", help="Set the path to the data",
                            type=str, metavar="(path to data file (.gz))")
        # do not use stopwords list
        parser.add_argument("--nostopwords", help="Disable stop words",
                            action="store_false")
        # path to new stopwords
        parser.add_argument("--stopwords", help="Set the path to stop words List", 
                            type=str, metavar="(path to stopwords list)")
        # minimum word size
        parser.add_argument("--word_size", help="Set the maximum for the word size filter", 
                            type=int, metavar="(integer number)")
        # no minimum word size
        parser.add_argument("--no_word_size", help="Disable word size filter",
                            action="store_false")
        # do not use stemmer
        parser.add_argument("--no_stemmer", help="Disable stemmer",
                            action="store_false")
        # do not use positions
        parser.add_argument("--use_positions", help="Enable positions indexing",
                            action="store_true")
        # maximum postings per block for the SPIMI
        parser.add_argument("--max_post", help="Set the maximum postings per block",
                            type=int)


        ##IF IS QUERY MODE
        #set folder name
        parser.add_argument("--data", help="Set folder name", 
                            type=str)
        # set the search mode
        parser.add_argument("--search_type", help="Choose the search mode", 
                            type=str, metavar="file/loop")
        # Set the query file
        parser.add_argument("--query_file", help="Choose the path to search", 
                            type=str, metavar="(txt file)")

        return parser

    def check_arguments(self, parser, args):

        if args.mode  == 'indexer':
            #indexer
            self.mode = args.mode
            #method
            if args.method:
                if args.method == 'bm25' or args.method == 'lnc.ltc' or args.method == 'raw':
                    self.index_type = args.method
                else:
                    parser.error("--method requires 3 options (raw / lnc.ltc / bm25).")
            else:
                parser.error("Indexer mode requires --method and --data_path.")

            #data_path
            if args.data_path:
                self.data_path = args.data_path
                if not path.exists(self.data_path) or not self.data_path.endswith('.gz'):
                    print("File does not exist or does not have the correct extension! ")
                    print(parser.parse_args(['-h']))
                    sys.exit()
            else:
                parser.error("Indexer mode requires --method and --data_path.")

            # if stopwords are disabled but a stopwords path is still defined by the user
            if (not args.nostopwords) and (args.stopwords != None):
                print(parser.parse_args(['-h']))
                sys.exit()

            if not args.nostopwords:
                self.stopwords_path = ''

            if args.stopwords:
                self.stopwords_path = args.stopwords

            # if word size is disabled but a size is still defined by the user
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

        elif args.mode == 'searcher':
            #searcher
            self.mode='searcher'

            #data_path
            if args.data:
                self.data = args.data
                if not path.exists('index/' + self.data) :
                    print("Folder does not exist!")
                    sys.exit()
            else:
                parser.error("Search mode requires --data and --search_type.")

            #search_type
            if args.search_type == 'loop':
                self.loop = True
            elif args.search_type == 'file':
                self.file = args.search_type
                if not args.query_file:
                    parser.error("Type of search by file required  --query_file.")
                self.query_file = args.query_file
                if not path.exists(self.query_file) :
                    print("Query file does not exist!")
                    sys.exit()  
            else:
                parser.error("Search type requires one of two options: file / loop.")
                
        else:
            print(parser.parse_args(['-h']))

    def read_query_file(self):
        
        with open(self.query_file, 'r') as file:
            lines=file.readlines() 
        return lines

    def main(self):

        # create and check all arguments
        parser = self.parse_args()
        args = parser.parse_args()
        self.check_arguments(parser, args)



        if self.mode =='indexer':
            if self.index_type == 'bm25':
                tokenizer = Tokenizer(stopwords_path = self.stopwords_path, 
                                        stemmer_enabled = self.stemmer_enabled, 
                                        size_filter = self.minimum_word_size)
                indexer = IndexerBM25(tokenizer, index_type=self.index_type, use_positions=self.use_positions)
                indexer.index_data_source(self.data_path)
            elif self.index_type == 'lnc.ltc':
                pass 
            

        elif  self.mode == 'searcher':
            query = Query(data_path = self.data)
            if self.loop:
                print("Words to search:")
                to_search = input()
                while ( to_search != '0'):
                    #query = Query(data_path = self.data)
                    query.process_query(to_search)
                    print("Words to search:")
                    to_search = input()

            if self.file:
                lines = self.read_query_file()
                for line in lines:
                    print("\n Q: {}".format(line))
                    query.process_query(line)



if __name__ == "__main__":    
    
    Main().main()
