# IR-Project1

## The goals

Create a document indexer using the SPIMI approach. Implement two alternative indexing limits in SPIMI: number of posts and amount of memory used.



## Usage
```
usage: python3 src/main.py [-h] --data_path path to data file (.gz)) [--nostopwords] [--stopwords (path to stopwords list)] [--word_size (integer number] [--no_word_size] [--no_stemmer] [--use_positions] [--max_post]

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
  --max_post MAX_POST   Set the maximum postings per temp block
```

* The data_path option is to introduce a data file
* The nostopwords option is to identify not to use the stopwords list.
* The stopwords options is to identify a new stopword list, by default we use this [stopword list](content/stopwords.txt).
* The word_size option is to set another minimum words size, by defaut the size is 3.
* The no_word_size option is to identify not to use a word size filter.
* The no_stemmer option is to identify not to use semmers words.
* The use_positions option is to identify to use the position list, by default we will not use.


## Example to run

By default
```
python3 src/main.py --data_path (path)
```

With new data file and without stopwords list
```
python3 src/main.py --data_path (path) --nostopwords
```


## Operation

Initially, open the file and go through chunks of lines, using the filters introduced by the user let's choose the words (tokenizer) and store all the data in files (indexer), called TEMPBLOCKS.
After these TEMPBLOCKS created, we will add them in order to make the search more easier, where at this stage 3 files will be created:
* DocKeys - is a review_id (documents) dictionary, where with an integer number, we identify the document
* MasterIndex - corresponds to the file that contains all the terms that were introduced by the data file, and it identifies the name of the file in which the term is located, in order to make the search faster.
* PostingIndexBlock - corresponds to a list of documents, which will contain the ordered terms.

After creating all the files, it is necessary to enter the search word.
The process will end when the value 0 is entered 

## The results

The first results refer to the values that took the indexing process
In this case, the file amazon_reviews_us_Digital_Music_Purchase_v1_00.tsv.gz return

| Data source | Nr. of indexed documents | Nr. of postings | Vocabulary size | Total indexing time (s) | Total index size on disk (MB) | Number of temporary index segments | Time to set up a query searcher |
|:-------------:|:-------------:|:-------------:|:-------------:|:-------------:|:-------------:|:-------------:|:-------------:|
|test|0|0|0|0|0|0|0|

    Total indexing time (s): 184.74614882469177
    Total index size on disk (bytes): 299375435
    Vocabulary size: 1688884
    Number of temporary index segments: 34
    Number of indexed documents: 1688884
    Number of postings: 33489189

After return this values, the user need to enter the word to search (this example the word to search is "test"):

    Word to Search:
    test
    Entered Word: test
    Normalized word:test
    Doc frequency: 3652
    File to Search: 22


## Authors

João Pedro Pereira - 106346 [GitHub](https://github.com/joaopedropereiraPP)  
Ivo Félix - 109641 [GitHub](https://github.com/IvoFelix) 



