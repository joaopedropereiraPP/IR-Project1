# IR-Project1

## The goals

Create a document indexer using the SPIMI approach. Implement two alternative indexing limits in SPIMI: number of posts and amount of memory used.



## Usage
```
usage: main.py [-h] [--data_path path to data file (.gz))] [--nostopwords] [--stopwords (path to stopwords list)] [--word_size (integer number] [--no_word_size] [--no_stemmer] [--use_positions]

optional arguments:
  -h, --help            show this help message and exit
  --data_path (path to data file (.gz))
                        Set the path to the data
  --nostopwords         Set not to use Stop Words List
  --stopwords (path to stopwords list)
                        Set the path to Stop Words List
  --word_size (integer number)
                        Set the minimum words size
  --no_word_size        Set not to use minimum words size
  --no_stemmer          Set not to use Stemmer
  --use_positions       Set to use positions
```

* The data_path option is to introduce a data file, by default it will use the ["content/amazon_reviews_us_Digital_Video_Games_v1_00.tsv.gz"](content/amazon_reviews_us_Digital_Video_Games_v1_00.tsv.gz).
*The nostopwords option is to identify not to use the stopwords list.
*The stopwords options is to identify a new stopword list, by defaul we use this [stopword list](content/stopwords.txt).
*The word_size option is to set another minimum words size, by defaut the size is 3.
*The no_word_size option is to identify not to use a word size filter.
*The no_stemmer option is to identify not to use semmers words.
*The use_positions option is to identify to use the position list, by default we will not use.

## Authors

João Pedro Pereira - 106346 [GitHub](https://github.com/joaopedropereiraPP)  
Ivo Félix - 109641 [GitHub](https://github.com/IvoFelix) 



