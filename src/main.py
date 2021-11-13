from tokenizer import Tokenizer
from indexer import Indexer
import gzip
import csv
import sys


class Principle:
    def __init__(self,stopwords_path) :

        self.tokenizer = Tokenizer(stopwords_path)
        self.indexer = Indexer()
        self.size=0
        
    
    def gen_chunks(self, reader, chunksize):
        """ SOME HELP: https://stackoverflow.com/questions/4956984/how-do-you-split-reading-a-large-csv-file-into-evenly-sized-chunks-in-python """
        chunk = []
        for i, line in enumerate(reader):
            if (i % chunksize == 0 and i > 0):
                yield chunk
                del chunk[:]  # or: chunk = []
            chunk.append(line)
        yield chunk

    def initilize(self, use_size_filter, tokenizer, stemmer = True, chunksize = 2) :
        maxInt = sys.maxsize

        csv.field_size_limit(maxInt)
        reviews=[]

        #original_file = "content/amazon_reviews_us_Digital_Video_Games_v1_00.tsv.gz"
        original_file = "content/data.tsv.gz"
        
        with gzip.open(original_file, "rt") as tsv_file:
            reader = csv.DictReader(tsv_file, delimiter="\t")
            
            
            for chunk in self.gen_chunks(reader, chunksize):
                print(" \tNEW CHUNK")
                tokens = []

                #Tokenizer for every chunks
                for row in chunk:
                    
                    identification = row['review_id']
                    text_value = row['product_title']  + " " + row['review_headline']  + " " + row['review_body'] 
                    print(text_value)
                    tokens += self.tokenizer.tokenize(text_value, identification, use_size_filter, tokenizer, stemmer)
                """
                1º Realizar index para o bloco de tokens do chunk (pedaço)
                2º Criar bloco (ficheiro) e escrever ( term: postings )
                    Ou seja, vai ser criado
                """

    
            #tokens = sorted(tokens)
            #Indexer
            self.indexer.index(tokens)
            #print("------------####################-----------------")
            for x in self.indexer.get_indexed().keys():
                #print(x)
                """print(type(self.indexer.get_indexed()[x]['posting_list']))
                print("documents_frequency: "+ str(self.indexer.get_indexed()[x]['documents_frequency']))
                """
            print("*************************************************")




def usage():
    print("Usage: python3 src/main.py \n\t <tokenizer_mode: default/ignore/normal> \n\t <token_length: int> \n\t <stopwords_list: default / path> \n\t <stemmer_use: yes/no>")

#Usage : python main.py
#<tokenizer_mode: default/ignore/normal> 
#<token_length: int>
#<stopwords_list: default / path>
#<stemmer_use: yes/no>


if __name__ == "__main__":    
    

    if len(sys.argv) > 4:
        tokenizer = sys.argv[1]
        tokenizer_length = int(sys.argv[2])
        stopwords_path = str(sys.argv[3])
        use_stemmer = str(sys.argv[4])
    else:
        usage()
        sys.exit(1)

 
    if tokenizer == 'default':
        tokenizer_length = 3
        use_size_filter = True
    elif tokenizer == 'ignore':
        use_size_filter = False
    elif tokenizer == 'normal':
        use_size_filter = True
    else:
        usage()
        sys.exit(1)

    if stopwords_path == 'default':
        stopwords_path = "content/stopwords.txt"  
    elif stopwords_path == 'none':
        stopwords_path = ''

    if use_stemmer in ["yes" , "y", "1"]:
        stemmer = True
    elif use_stemmer in ["no" , "n", "0"]:
        stemmer = False
    else :
        usage()
        sys.exit()

    principle = Principle(stopwords_path)

    principle.initilize(use_size_filter, tokenizer_length, stemmer)
