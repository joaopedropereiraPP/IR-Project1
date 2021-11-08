from tokenizer import Tokenizer
from indexer import Indexer
import gzip
import csv
import sys
import re
#import psutil

class Principle:
    def __init__(self,stopwords_path) :

        self.tokenizer = Tokenizer(stopwords_path)
        self.indexer = Indexer()
        self.size=0
        
    def initilize(self, use_size_filter, tokenizer, stemmer = True) :
        maxInt = sys.maxsize

        csv.field_size_limit(maxInt)
        reviews=[]

        #original_file = "/home/pedro/Desktop/RI/Assignment_01/content/amazon_reviews_us_Digital_Video_Games_v1_00.tsv.gz"
        original_file = "/home/pedro/Desktop/RI/Assignment_01/content/data.tsv.gz"
        
        with gzip.open(original_file, "rt") as tsv_file:
            reader = csv.DictReader(tsv_file, delimiter="\t")
            tokens = []
            #print(list(reader))
   
            #Tokenizer
            for row in reader:
                identification = row['review_id']
                text_value = row['product_title']  + " " + row['review_headline']  + " " + row['review_body'] 
                tokens += self.tokenizer.tokenize(text_value, identification, use_size_filter, tokenizer, stemmer)
    
            
            #Indexer
            self.indexer.index(tokens)
            print("-------------")
            print(self.indexer.get_indexed())



def usage():
    print("Usage: python3 main.py \n\t <tokenizer_mode: default/ignore/normal> \n\t <token_length: int> \n\t <stopwords_list: default / path> \n\t <stemmer_use: yes/no>")

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
        stopwords_path = "../content/stopwords.txt"  
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
