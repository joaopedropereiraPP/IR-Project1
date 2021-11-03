from tokenizer import Tokenizer
import gzip
import csv
import sys
import re
#import psutil

class Principle:
    def __init__(self) :
        self.tokenizer = Tokenizer()
        #self.indexer = Indexer(positional_flag=positional_flag)
        #self.ranker = Ranker(queries_path=queries_path ,mode=rank_mode,docs_limit=docs_limit)
        #self.file = file

    def initilize(self) :
        maxInt = sys.maxsize

        csv.field_size_limit(maxInt)
        reviews=[]

        #original_file = "/home/pedro/Desktop/RI/Assignment_01/content/amazon_reviews_us_Digital_Video_Games_v1_00.tsv.gz"
        original_file = "/home/pedro/Desktop/RI/Assignment_01/content/data.tsv.gz"
        
        with gzip.open(original_file, "rt") as tsv_file:
            reader = csv.DictReader(tsv_file, delimiter="\t")
            tokens = []
            for row in reader:
                identification = row['review_id']
                text_value = row['review_headline']  + " " + row['review_body'] 
                tokens += self.tokenizer.tokenize(text_value, identification)
            
            for token in tokens:
                print(token)
                #reviews.append({'review_id' : row['review_id'] , 'text':row['review_headline']  + " " + row['review_body'] })
        #words = re.sub("[^a-zA-Z]+"," ", reviews[0]['text']).lower().split(" ")
        #print(words)



if __name__ == "__main__":    
        
    principle = Principle()

    principle.initilize()
