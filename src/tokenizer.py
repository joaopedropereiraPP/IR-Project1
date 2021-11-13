import re
import snowballstemmer

class Tokenizer:
    def __init__(self, stopwords_path ):
        #stemmer
        self.stemmer = snowballstemmer.stemmer('english')

        #initilize stopwords
        if stopwords_path != '':
            text = open(stopwords_path,'r')
            self.stopwords = [word.strip() for word in text.readlines()]
        else:
            self.stopwords = []
        
    def tokenize(self,input_string,index, use_size_filter, tokenizer_length, stemmer = True):
        final_tokens = []

        #Separe all words
        tokens = re.sub("\-+","",input_string)
        tokens = re.sub("[^0-9a-zA-Z]+"," ",input_string).lower().split(" ") 

        
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
        
        #Create final tokens ('term', 'review_id', 'position')
        for token in tokens: 
            final_tokens.append((token[0],index, token[1])) 


        return final_tokens