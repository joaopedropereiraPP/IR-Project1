from tokenizer import Tokenizer
from indexer import Indexer
from query import Query

#term_to_search = input()
#query = Query()
#query.process_query(term_to_search)



dataset_name = 'teste'
dataset_name = 'amazon_reviews_us_Digital_Video_Games_v1_00'

query = Query(data_path = dataset_name)
word = 'yuri'
query.process_query(word)


        
#initial_time = time()

#query.read_master_index()

#print("Time to set up a query searcher (s): " + str(time() - initial_time) )
#print("Search term:")
#word_to_search = input()
#while word_to_search != '0':
    
#    query.process_query(word_to_search)
#    print("Search term ( 0 to exit ):")
#    word_to_search = input()