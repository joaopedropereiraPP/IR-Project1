import re
from typing import Tuple
import Stemmer

class Tokenizer:
    def __init__(self, stopwords_path ):
        #stemmer
        self.stemmer = Stemmer.Stemmer('english')

        #initilize stopwords
        if stopwords_path != '':
            text = open(stopwords_path,'r')
            self.stopwords = [word.strip() for word in text.readlines()]
        else:
            self.stopwords = []
        
    def tokenize(self,input_string,index, use_size_filter, tokenizer_length, stemmer = True):
        print("###################TOKEN INICIAL ########################")
        final_tokens = []

        #Separe all words
        tokens = re.sub("[^0-9a-zA-Z]+"," ",input_string).lower().split(" ") # Make some changes here, having into account that this is a biomedical corpus
       
        #remove stopwords  
        if use_size_filter == True:
            tokens = [token for token in tokens if len(token)>tokenizer_length and token not in self.stopwords]
        else:
            tokens = [token for token in tokens if token not in self.stopwords]
        
        #Stemmer Words
        if stemmer ==True:
            tokens = self.stemmer.stemWords(tokens)
      

        #Define position
        tokens = [ (tokens[i],i) for i in range(0,len(tokens)) ]
        
        # Iterate over each word in line 
        for token in tokens: 
            # if it passes the condition, we shall add it to the final_tokens
            final_tokens.append((token[0],index, token[1])) #token 1 represents its position
        print(final_tokens)
        print("###################TOKEN FIM ########################")
        return final_tokens