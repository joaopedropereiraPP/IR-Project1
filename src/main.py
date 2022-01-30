import sys
from argparse import ArgumentParser
from os import path

from indexer import Indexer
from indexer_bm25 import IndexerBM25
from indexer_lnc_ltc import IndexerLncLtc
from query import Query
from tokenizer import Tokenizer


class Main:
    data_path: str
    stopwords_path: str
    minimum_word_size: int
    stemmer_enabled: bool
    use_positions: bool
    parser: ArgumentParser
    tokenizer: Tokenizer
    indexer: Indexer

    def __init__(self):

        # Main Mode
        self.mode = ''

        # Indexer mode
        self.index_type = ''
        self.data_path = ''
        self.stopwords_path = 'content/stopwords.txt'
        self.minimum_word_size = 3
        self.stemmer_enabled = True
        self.use_positions = False
        self.max_post = 1000000

        # searcher mode
        self.data = ''
        self.search_type = ''
        self.loop = False
        self.query_file = ''
        self.dump_results_file = False
        self.cmd_results = False

        self.parser = ArgumentParser()
        self.tokenizer = Tokenizer(stopwords_path=self.stopwords_path,
                                   stemmer_enabled=self.stemmer_enabled,
                                   size_filter=self.minimum_word_size)
        self.indexer = Indexer(tokenizer=self.tokenizer,
                               max_postings_per_temp_block=self.max_post,
                               use_positions=self.use_positions)

    def parse_args(self):
        parser = ArgumentParser()
        # Set the mode
        parser.add_argument('--mode', help='Set the main mode', required=True,
                            type=str, metavar='indexer/searcher')

        # IF IS INDEXER MODE
        # set method
        parser.add_argument('--method', help='Set the method',
                            type=str, metavar='raw/lnc.ltc/bm25')
        # path to new data file
        parser.add_argument('--data_path', help='Set the path to the data, it should be relative to the program directory',
                            type=str, metavar='(path to data file (.gz))')
        # do not use stopwords list
        parser.add_argument('--nostopwords', help='Disable stop words',
                            action='store_false')
        # path to new stopwords
        parser.add_argument('--stopwords',
                            help='Set the path to stop words List',
                            type=str, metavar='(path to stopwords list)')
        # minimum word size
        parser.add_argument('--word_size',
                            help='Set the maximum for the word size filter',
                            type=int, metavar='(integer number)')
        # no minimum word size
        parser.add_argument('--no_word_size', help='Disable word size filter',
                            action='store_false')
        # do not use stemmer
        parser.add_argument('--no_stemmer', help='Disable stemmer',
                            action='store_false')
        # do not use positions
        parser.add_argument('--use_positions',
                            help='Enable positions indexing',
                            action='store_true')
        # maximum postings per block for the SPIMI
        parser.add_argument('--max_post',
                            help='Set the maximum postings per block',
                            type=int)

        # IF IS QUERY MODE
        # set folder name
        parser.add_argument('--data', help="Folder that contains the index files for query mode, it should be a folder inside the 'index' subfolder of the program",
                            type=str)
        # set the search mode
        parser.add_argument('--search_type', help="Choose the search mode, 'file (file-path)' to use a file with a list of queries as input, 'loop' to insert queries in a loop through the terminal (empty query to end loop)",
                            nargs='+', metavar='file (file-path)/loop')

        parser.add_argument('--dump_file',
                            help='Enable to generate file with results',
                            action='store_true')

        parser.add_argument('--cmd_results',
                            help='Enable to show the results on terminal',
                            action='store_true')
        # Set the query file
        #parser.add_argument('--query_file', help='Choose the path to search', type=str, metavar='(txt file)')

        return parser

    def check_arguments(self, parser, args):

        if args.mode == 'indexer':
            # indexer
            self.mode = args.mode
            # method
            if args.method:
                if args.method == 'bm25' or args.method == 'lnc.ltc' or args.method == 'raw':
                    self.index_type = args.method
                else:
                    parser.error(
                        '--method requires 3 options (raw / lnc.ltc / bm25).')
                    sys.exit()
            else:
                parser.error('Indexer mode requires --method and --data_path.')
                sys.exit()

            # data_path
            if args.data_path:
                self.data_path = args.data_path
                if not path.exists(self.data_path) or not self.data_path.endswith('.gz'):
                    print(
                        'File does not exist or does not have the correct extension! ')
                    print(parser.parse_args(['-h']))
                    sys.exit()
            else:
                parser.error('Indexer mode requires --method and --data_path.')
                sys.exit()

            # if stopwords are disabled but a stopwords path is still defined by the user
            if (not args.nostopwords) and (args.stopwords != None):
                print(parser.parse_args(['-h']))
                sys.exit()

            if not args.nostopwords:
                self.stopwords_path = ''

            if args.stopwords:
                self.stopwords_path = args.stopwords

            # if word size is disabled but a size is still defined by the user
            if (not args.no_word_size) and (args.word_size != None):
                print(parser.parse_args(['-h']))
                sys.exit()

            if args.word_size:
                self.minimum_word_size = args.word_size

            if not args.no_word_size:
                self.minimum_word_size = 0

            if not args.no_stemmer:
                self.stemmer_enabled = False

            if args.use_positions:
                self.use_positions = True

            if args.max_post:
                self.max_post = args.max_post

        elif args.mode == 'searcher':
            # searcher
            self.mode = 'searcher'

            # data_path
            if args.data:
                self.data = args.data
                if not path.exists(self.data):
                    print('Folder does not exist!')
                    sys.exit()
            else:
                parser.error('Search mode requires --data and --search_type.')
                sys.exit()

            # search_type
            if args.search_type:
                if args.search_type[0] == 'loop':
                    self.loop = True
                elif args.search_type[0] == 'file':
                    if not args.search_type[1]:
                        parser.error(
                            'Type of search by file required the file path (txt)')
                        sys.exit()
                    self.query_file = args.search_type[1]
                    if not path.exists(self.query_file):
                        print('Query file does not exist!')
                        sys.exit()
                else:
                    parser.error(
                        'Search type requires one of two options: file / loop.')
                    sys.exit()
            else:
                parser.error(
                    'Search type requires one of two options: file / loop.')
                sys.exit()

            if not args.dump_file and not args.cmd_results:
                parser.error(
                    'Search type requires at least one of two options: --dump_file / --cmd_results')
                sys.exit()

            self.dump_results_file = args.dump_file
            self.cmd_results = args.cmd_results
        else:
            print(parser.parse_args(['-h']))

    def read_query_file(self):

        with open(self.query_file, 'r') as file:
            lines = file.readlines()
        return lines

    def main(self):

        # create and check all arguments
        parser = self.parse_args()
        args = parser.parse_args()
        self.check_arguments(parser, args)

        if self.mode == 'indexer':
            if self.index_type == 'bm25':
                tokenizer = Tokenizer(stopwords_path=self.stopwords_path,
                                      stemmer_enabled=self.stemmer_enabled,
                                      size_filter=self.minimum_word_size)
                indexer = IndexerBM25(
                    tokenizer,  use_positions=self.use_positions)
                indexer.index_data_source(self.data_path)
                statistics = indexer.get_statistics()
                for statistic in statistics:
                    print(f'{statistic}: {statistics[statistic]}')
            elif self.index_type == 'lnc.ltc':
                tokenizer = Tokenizer(stopwords_path=self.stopwords_path,
                                      stemmer_enabled=self.stemmer_enabled,
                                      size_filter=self.minimum_word_size)
                indexer = IndexerLncLtc(
                    tokenizer,  use_positions=self.use_positions)
                indexer.index_data_source(self.data_path)
                statistics = indexer.get_statistics()
                for statistic in statistics:
                    print(f'{statistic}: {statistics[statistic]}')

        elif self.mode == 'searcher':
            query = Query(
                data_path=self.data, dump_results_file=self.dump_results_file, cmd_results=self.cmd_results)
            if self.loop:
                print('Words to search: ')
                to_search = input()
                while (to_search != ''):
                    #query = Query(data_path = self.data)
                    query_result = query.process_query(to_search)
                    if self.cmd_results:
                        self.show_results(to_search, query_result)
                    print('Words to search:')
                    to_search = input()

            else:
                lines = self.read_query_file()
                for line in lines:
                    print('\n Q: {}'.format(line))
                    query_result = query.process_query(line)
                    if self.cmd_results:
                        self.show_results(line.replace("\n", ""), query_result)


    def show_results(self, query, results):
        i = 0
        print('Q: {}'.format(query))
        for result in results:
            if i < 10:
                print(result)
            i += 1
        print("\n")


if __name__ == '__main__':

    Main().main()
