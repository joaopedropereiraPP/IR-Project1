from tokenizer import Tokenizer

tokenizer = Tokenizer(stopwords_path='', stemmer_enabled=True, size_filter=0)
input_str = 'Some test phrase!'
tokens = tokenizer.tokenize(input_str)
print(tokens)
