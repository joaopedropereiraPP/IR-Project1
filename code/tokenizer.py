import re
import Stemmer

class Tokenizer:
    def __init__(self):
        #stemmer
        self.stemmer = Stemmer.Stemmer('english')

        #initilize stopwords
        stopwords_file = "/home/pedro/Desktop/RI/Assignment_01/content/stopwords.txt"   
        text = open(stopwords_file,'r')
        self.stopwords = [word.strip() for word in text.readlines()]

    
    # Function to read any text and add it to the word dictionary of the Tokenizer
    def tokenize(self,input_string,index):
        final_tokens = []

        #Separe all words
        tokens = re.sub("[^0-9a-zA-Z]+"," ",input_string).lower().split(" ") # Make some changes here, having into account that this is a biomedical corpus
      
        #remove stopwords  
        #Word Z 3 e não conter na lista das stopwords
        tokens = [token for token in tokens if len(token)>3 and token not in self.stopwords]

        #Stemmer Words
        tokens = self.stemmer.stemWords(tokens)
      

        #Define position
        tokens = [ (tokens[i],i) for i in range(0,len(tokens)) ]

        # Iterate over each word in line 
        for token in tokens: 
            # if it passes the condition, we shall add it to the final_tokens
            final_tokens.append((token[0],index, token[1])) #token 1 represents its position

        return final_tokens