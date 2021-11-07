from tokenizer import Tokenizer
import gzip
import csv
import sys
import re
#import psutil

class Principle:
    def __init__(self) :
        self.tokenizer = Tokenizer()


    def initilize(self, use_size_filter, tokenizer) :
        maxInt = sys.maxsize

        csv.field_size_limit(maxInt)
        reviews=[]

        #original_file = "/home/pedro/Desktop/RI/Assignment_01/content/amazon_reviews_us_Digital_Video_Games_v1_00.tsv.gz"
        original_file = "/home/pedro/Desktop/RI/Assignment_01/content/data.tsv.gz"
        
        with gzip.open(original_file, "rt") as tsv_file:
            reader = csv.DictReader(tsv_file, delimiter="\t")
            tokens = []
            print(reader)
            for row in reader:
                identification = row['review_id']
                text_value = row['review_headline']  + " " + row['review_body'] 
                print(text_value)
                tokens += self.tokenizer.tokenize(text_value, identification, use_size_filter, tokenizer)
            
            for token in tokens:
                print(token)
                #reviews.append({'review_id' : row['review_id'] , 'text':row['review_headline']  + " " + row['review_body'] })
        #words = re.sub("[^a-zA-Z]+"," ", reviews[0]['text']).lower().split(" ")
        #print(words)

def usage():
    print("Usage: python3 main.py \n\t <tokenizer_mode: default/ignore/normal> \n\t <token_length: int>")



if __name__ == "__main__":    
    

    if len(sys.argv) > 2:
        tokenizer = sys.argv[1]
        tokenizer_length = int(sys.argv[2])
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



    principle = Principle()

    principle.initilize(use_size_filter, tokenizer_length)
