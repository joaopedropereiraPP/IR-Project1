# IR-Project3

## The goal

Create a document indexer using the SPIMI approach and a basic searching mechanism. Three types of indexes are supported: raw (no term weighting), lnc.ltc and BM25, with term positions indexing and boosting based on term proximity and similarity to the query, with the option to disable this behavior. For all index types except raw there is also a query mechanism that returns ranked results.

## Design

The application was completely developed in Python, using an object-oriented approach, to better guarantee encapsulation and modularity, with type annotations for all class members and methods to improve readability and unit tests for classes. It has the option of indexing with term positions, though this is disabled by default.

The SPIMI indexing limit per block that was implemented is based on number of postings, while the tokenizer implementation performs string preprocessing, stopword removal, word size filtering and stemming, which are all done in a single pass to prevent iterating each document multiple times.

It is prepared to parse Amazon review data files as the collection of documents to index, which follow the structure described on the beginning of this document: https://s3.amazonaws.com/amazon-reviews-pds/tsv/index.txt. 

All index files are stored uncompressed as TSV files on the `index/data_source_name` subfolder, with the following structure:
* PostingIndex#.tsv - the final index files. Multiple files are created if the SPIMI posting limit per block is reached, where `'#'` is the block number. It contains the term on the first column of each row, and a posting on each subsequent column, as its document ID, which for weighted indexes is followed by the character `':'` and the posting weight and if positions are enabled by another`':'` and the list of positions on the document separated by `','`.
* TempBlock#.tsv - temporary index blocks used for merging into the final index. Multiple files are created if the SPIMI posting limit per block is reached, where `'#'` is the block number. The structure is the same as for the final index. Though it isn't necessary these blocks are kept after the final index is created so that they can be inspected.
* MasterIndex.tsv - Contains the document frequency (IDF if it's a weighted index type) and the final index block number where it can be found. The terms are on the first column of each row, followed on each column by its document frequency and the block number of the posting index.
* DocKeys.tsv - contains the correspondence of surrogate keys to natural keys, that is, the keys generated by the program and the original hexadecimal keys from Amazon, as well as the document title.

The BM25 weights are calculated for each posting already containing the multiplication by the inverted document frequency (IDF), in this manner it is intended to optimize the performance on the side of the queries while losing some performance while indexing, so that the program is more responsive on the side of the user.

The logarithms for the calculation of the weights and IDF are the most expensive operation among the indexing tasks, so this was optimized through a dynamic programming approach to resuse previous calculations with the same input for the logarithm, as the inputs are integer which are likely to be repeated. For the IDF the calculation was divided into two logarithms (log(N) - log(DF)), so that all logarithm calculations have an integer as input.

The base class for the index types is the raw index type, for which there are subclasses for the lnc.ltc index type and the BM25 index type. An hierarchy of posting classes was also created to abstract the structure of the index and how each posting is written to the disk and parsed back to memory for all index types and positional and nonpositional indexes.

The positional boosting function depends on an upper limit, the number of terms in the considered span, number of words between the most distant terms, similarity between the query and the terms in the span (considering terms and order, using gestalt pattern matching as implemented in the Python difflib module). It is quadratic as a function of the span size, with a value of 0 when the maximum span size is reached, as follows:

nr_terms_in_span / span_size * query_similarity * max_boost

## Prerequisites

The program was developed using Python 3.8, and in addition the PyStemmer module is required (https://github.com/snowballstem/pystemmer), which is a C implementation of snowballstemmer, and was chosen due to its improved performance compared to the default implementation of snowballstemmer.
Install it with the command:
```
pip install PyStemmer
```
## Usage
```
usage: main.py [-h] --mode indexer/searcher [--method raw/lnc.ltc/bm25] [--data_path path to data file (.gz))] [--nostopwords] [--stopwords (path to stopwords list)] [--word_size (integer number)] [--no_word_size] [--no_stemmer]
               [--use_positions] [--max_post MAX_POST] [--data DATA] [--search_type file (file-path)/loop [file (file-path/loop ...]] [--dump_file] [--cmd_results]

optional arguments:
  -h, --help            show this help message and exit
  --mode indexer/searcher
                        Set the main mode
  --method raw/lnc.ltc/bm25
                        Set the method
  --data_path (path to data file (.gz))
                        Set the path to the data
  --nostopwords         Disable stop words
  --stopwords (path to stopwords list)
                        Set the path to stop words List
  --word_size (integer number)
                        Set the maximum for the word size filter
  --no_word_size        Disable word size filter
  --no_stemmer          Disable stemmer
  --disable_positions   Disable positions indexing
  --max_post MAX_POST   Set the maximum postings per block
  --data DATA           Folder that contains the index files for query mode
  --search_type file (file-path)/loop [file (file-path)/ evaluation (file_path) ...]
                        Choose the search mode, 'file (file-path)' to use a file with a list of queries as input, 'loop' to insert queries in a loop through the terminal (empty query to end loop) and 'evaluation (file)' to evaluate retrieval engine using the relevance scores provided by input file
  --dump_file           Enable to generate file with results
  --cmd_results         Enable to show the results on terminal
  --disable_boost       Disable positional boosting of documents
  --span_size           Set the span value to use on boost 
```

* The data_path option is the path to the Amazon review data file to be indexed.
* The nostopwords option disables stopwords.
* The stopwords option sets the path to the stopwords list, which should be a simple text file with a stop word on each row. The default is the file in `content/stopwords.txt`, which was taken from http://www.search-engines-book.com/data/stopwords.
* The word_size option sets the word size filter value, words with smaller or equal size than the value set here are filtered. The defaut is 3.
* The no_word_size option disables the word size filter.
* The no_stemmer option disables stemming.
* The disable_positions option enables/disables term positions on the index, default is off.
* The disable_boost option enables/disables boost on the evaluation search_type, default is off.
* The span_size option sets the size of span to use on boost, default is 4.


After running the program the data file starts to be indexed using the SPIMI approach and the index files are created as described in the Design section. When it is done some statistics on the process are returned and the user is asked to enter the search term, for which the document frequency and final index file block number in which its postings are contained is returned, that is, the `#` in PostingIndex#.tsv, as described previously.

## Example

Using default parameters:
```
python3 src/main.py --data_path (path)
```

With new data file and stopwords disabled:
```
python3 src/main.py --data_path (path) --nostopwords
```

Indexer mode:
```
python3 src/main.py --mode indexer --method lnc.ltc --data_path content/amazon_reviews_us_Digital_Music_Purchase_v1_00.tsv.gz
```
Searcher mode with loop:
```
python3 src/main.py --mode searcher --data index/amazon_reviews_us_Digital_Music_Purchase_v1_00 --search_type loop --cmd_results
``` 
Searcher mode with query file and dumping results to file:
```
python3 src/main.py --mode searcher --data index/amazon_reviews_us_Digital_Music_Purchase_v1_00 --search_type file queries/queries.txt --dump_file
``` 
Searcher mode with evaluation, show results on terminal and boost is disabled:
```
python3 src/main.py --mode searcher --data index/amazon_reviews_us_Digital_Music_Purchase_v1_00 --search_type evaluation queries/queries.relevance.txt --cmd_results --disable_boost
``` 
Searcher mode with evaluation, dumping results to file and boost is enabled:
```
python3 src/main.py --mode searcher --data index/amazon_reviews_us_Digital_Music_Purchase_v1_00 --search_type evaluation queries/queries.relevance.txt --dump_file 
``` 

## The results

### Indexing

The sample amazon_reviews_us_Digital_Music_Purchase_v1_00.tsv.gz from https://s3.amazonaws.com/amazon-reviews-pds/readme.html was used.

#### lnc.ltc index type

Configuration:
* index_type	lnc.ltc
* size_filter	3
* stemmer_enabled	True
* stopwords_path	content/stopwords.txt
* use_positions	False

Statistics:
* Number of indexed documents: 1688884
* Number of postings: 33469012
* Vocabulary size: 271867
* Total indexing time (s): 478.55587339401245
* Total index size on disk (MB): 993.258436
* Number of temporary index segments: 34

#### BM25 index type

Configuration:
* index_type	bm25
* size_filter	3
* stemmer_enabled	True
* stopwords_path	content/stopwords.txt
* use_positions	False

Statistics:
* Number of indexed documents: 1688884
* Number of postings: 33469012
* Vocabulary size: 271867
* Total indexing time (s): 471.712206363678
* Total index size on disk (MB): 969.322838
* Number of temporary index segments: 34

The ranking results for each index type were tested for the list of queries on the file `queries/queries.txt` and the results for each are on the subfolder `results`.

These results were produced on a machine with the following specifications:
* CPU: AMD Ryzen 5 4600H with Radeon Graphics
* RAM: 8GB
* Storage: SSD - Samsung MZALQ512HALU-000L2

### Querying

There were improvements by using positional boosting to most query metrics, especially for lnc.ltc. The default values defined in the program were used for these results, with 0.02 as the upper limit for boosting and a max span size of 4.

To stabilize time metrics each query in the silver standard was performed 10 times.

#### lnc.ltc without positional boosting
TOP 10 

* Precision: 0.76
* Recall: 0.10
* F-measure: 0.18
* Average precision: 0.10
* NDCG: 0.64
* Average query throughput (queries/s): 0.43
* Median query latency (ms): 2891.84

TOP 20 

* Precision: 0.66
* Recall: 0.18
* F-measure: 0.28
* Average precision: 0.16
* NDCG: 0.61
* Average query throughput (queries/s): 0.43
* Median query latency (ms): 2891.84

TOP 50 
* Precision: 0.53
* Recall: 0.35
* F-measure: 0.42
* Average precision: 0.29
* NDCG: 0.58
* Average query throughput (queries/s): 0.43
* Median query latency (ms): 2891.84

#### lnc.ltc with positional boosting
TOP 10 
* Precision: 0.79
* Recall: 0.11
* F-measure: 0.19
* Average precision: 0.10
* NDCG: 0.67
* Average query throughput (queries/s): 0.33
* Median query latency (ms): 3220.43

TOP 20 
* Precision: 0.67
* Recall: 0.18
* F-measure: 0.29
* Average precision: 0.17
* NDCG: 0.64
* Average query throughput (queries/s): 0.33
* Median query latency (ms): 3220.43

TOP 50 
* Precision: 0.55
* Recall: 0.37
* F-measure: 0.44
* Average precision: 0.31
* NDCG: 0.61
* Average query throughput (queries/s): 0.33
* Median query latency (ms): 3220.43

#### BM25 without positional boosting

TOP 10 
* Precision: 0.76
* Recall: 0.10
* F-measure: 0.18
* Average precision: 0.09
* NDCG: 0.64
* Average query throughput (queries/s): 0.44
* Median query latency (ms): 2825.76

TOP 20 
* Precision: 0.70
* Recall: 0.19
* F-measure: 0.30
* Average precision: 0.17
* NDCG: 0.63
* Average query throughput (queries/s): 0.44
* Median query latency (ms): 2825.76

TOP 50 
* Precision: 0.54
* Recall: 0.37
* F-measure: 0.44
* Average precision: 0.30
* NDCG: 0.60
* Average query throughput (queries/s): 0.44
* Median query latency (ms): 2825.76

#### BM25 with positional boosting

TOP 10 
* Precision: 0.76
* Recall: 0.10
* F-measure: 0.18
* Average precision: 0.09
* NDCG: 0.65
* Average query throughput (queries/s): 0.34
* Median query latency (ms): 3077.21

TOP 20 
* Precision: 0.71
* Recall: 0.19
* F-measure: 0.30
* Average precision: 0.17
* NDCG: 0.63
* Average query throughput (queries/s): 0.34
* Median query latency (ms): 3077.21

TOP 50 
* Precision: 0.54
* Recall: 0.37
* F-measure: 0.43
* Average precision: 0.30
* NDCG: 0.60
* Average query throughput (queries/s): 0.34
* Median query latency (ms): 3077.21

## Authors

Ivo Félix - 109641 [GitHub](https://github.com/IvoFelix)

João Pedro Pereira - 106346 [GitHub](https://github.com/joaopedropereiraPP)
