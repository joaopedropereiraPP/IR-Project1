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
        self.data_path = ['path1' , 'path2', 'path3', 'path4']
        self.use_stopwords = True
        self.stopwords_path = 'content/stopwords.txt'
        self.minimum_word_size = 3
        self.use_word_size = True
        self.use_stemmer = True

        self.parser = ArgumentParser()

       # self.tokenizer = Tokenizer(stopwords_path)
        #self.indexer = Indexer()
        #self.size=0
        
    
    def gen_chunks(self, reader, chunksize):
        """ SOME HELP: https://stackoverflow.com/questions/4956984/how-do-you-split-reading-a-large-csv-file-into-evenly-sized-chunks-in-python """
        chunk = []
        for i, line in enumerate(reader):
            if (i % chunksize == 0 and i > 0):
                yield chunk
                del chunk[:]  # or: chunk = []
            chunk.append(line)
        yield chunk

    def process(self, chunksize = 2) :
        """All the process start here"""
        maxInt = maxsize

        field_size_limit(maxInt)
        reviews=[]

        #original_file = "content/amazon_reviews_us_Digital_Video_Games_v1_00.tsv.gz"
        original_file = "../content/data.tsv.gz"
        
        
        for file in self.data_path:
            with open(file, "rt") as tsv_file:
                reader = DictReader(tsv_file, delimiter="\t")
                
                
                for chunk in self.gen_chunks(reader, chunksize):
                    print(" \tNEW CHUNK")
                    tokens = []

                    #Tokenizer for every chunks
                    for row in chunk:
                        
                        identification = row['review_id']
                        text_value = row['product_title']  + " " + row['review_headline']  + " " + row['review_body'] 
                        print(text_value)
                        tokens += self.tokenizer.tokenize(text_value)
                        print(tokens)
                    """
                    1º Realizar index para o bloco de tokens do chunk (pedaço)
                    2º Criar bloco (ficheiro) e escrever ( term: postings )
                        Ou seja, vai ser criado
                    """

        
                #tokens = sorted(tokens)
                #Indexer
                #self.indexer.index(row['review_id'],tokens)
                #print("------------####################-----------------")
                #for x in self.indexer.get_indexed().keys():
                    #print(x)
                    """print(type(self.indexer.get_indexed()[x]['posting_list']))
                    print("documents_frequency: "+ str(self.indexer.get_indexed()[x]['documents_frequency']))
                    """
                print("*************************************************")
 
    def parse_args(self):
        
        parser = ArgumentParser()
        #PATH TO NEW DATA FILE
        parser.add_argument("--data_path", help="increase output verbosity", 
                            type=str, metavar="(path to data file)")
        #NOT USE STOPWORDS LIST
        parser.add_argument("--nostopwords", help="Define to not use Stop Words List",
                            action="store_false")
        #PATH TO NEW STOPWORDS
        parser.add_argument("--stopwords", help="Define path to Stop Words List", 
                            type=str, metavar="(path to stopwords list)")
        #MINIMUM WORD SIZE
        parser.add_argument("--word_size", help="Define minimum words size", 
                            type=str, metavar="(integer number)")
        #NO MINIMUM WORD SIZE
        parser.add_argument("--no_word_size", help="Define minimum words size",
                            action="store_false")
        #NOT USE STEMMER APPROACH
        parser.add_argument("--nostemmer", help="increase output verbosity",
                            action="store_false")

        return parser

    def check_arguments(self, parser, args):

        if args.data_path :
            self.data_path = [args.data_path]
            if not path.isfile(self.data_path[0]):
                print("File does not exist")
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
            self.use_stemmer = False


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
        print("use_stemmer: " + str(self.use_stemmer))
        

        process = self.process()

        #process.initilize(use_size_filter, tokenizer_length, stemmer)



if __name__ == "__main__":    
    Main().main()

    
