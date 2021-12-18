from os import path
import csv

class bm25:
    def __init__(self, k = 1.2, b = 0.75, positions = {}, dl =0, avdl=0, data_path='', doc_keys = {}, master_index = {}):
        self.k = k
        self.b = b
        self.positions = positions
        self.tf = len(positions)
        self.dl = dl
        self.avdl = avdl
 
        #path files
        self.posting_index_block_file = 'index/'+ path.basename(data_path).split('.')[0]+'/PostingIndexBlock{}.tsv'
        self.master_index_folder_path = 'index/' + path.basename(data_path).split('.')[0] + '/MasterIndex.tsv'
        self.doc_keys_folder_path = 'index/'  + path.basename(data_path).split('.')[0]  + '/DocKeys.tsv'
        self.configurations_folder_path = ('index/' + path.basename(data_path).split('.')[0] + '/conf.tsv')

        self.doc_keys= doc_keys
        self.master_index = master_index

        self.post_data = {}
        self.files_to_open = {}
        self.bm25_ranking = {}
    
    def bm25_without_positions(self):
        dividend = (self.k + 1) * self.tf
        B = (1 - self.b) + self.b * (self.dl / self.avdl)
        divider = (self.k * B) + self.tf
        return str(dividend / divider)

    def bm25_with_positions(self):
        dividend = (self.k + 1) * self.tf
        B = (1 - self.b) + self.b * (self.dl / self.avdl)
        divider = (self.k * B) + self.tf
        self.positions = ",".join(map(str, self.positions))

        return str(dividend / divider) + ":" + self.positions


    def bm25_search(self, terms):
        self.post_data = {}
        self.files_to_open = {}
        self.bm25_ranking = {}

        #Stores all documents with their terms to optimize the search within the PostingIndexBlock file 
        self.store_files_to_open(terms)

        for file_number in self.files_to_open.keys():
            
            file_name = self.posting_index_block_file.format(file_number)
            self.read_posting_index_block(file_name)

            index = 0
            for terms_on_file in self.files_to_open[file_number]:

                for any_term in terms_on_file.keys():
                    for docs in self.post_data[any_term].keys():
                        if docs not in self.bm25_ranking:
                            self.bm25_ranking[docs] = 0
                            idf = float(self.files_to_open[file_number][index][any_term]['idf'])
                            weight = float(self.post_data[any_term][docs])
                            count = int(self.files_to_open[file_number][index][any_term]['count'])
                        self.bm25_ranking[docs] += ((weight * idf) * count)
                    index += 1
        self.bm25_ranking = sorted(self.bm25_ranking.items(), key=lambda x: x[1], reverse=True) 
        print("BM25 Ranking")
        for i in range(0,3):
            if len(self.bm25_ranking) >= i + 1:
                doc=self.doc_keys[tuple(list(self.bm25_ranking)[i])[0]]['real_id']
                doc_name=self.doc_keys[tuple(list(self.bm25_ranking)[i])[0]]['doc_name']
                print("{}º: {}".format(i+1,  doc+" -> "+ doc_name))  
                print(self.bm25_ranking[i])  

    def read_configurations(self):
        with open(self.configurations_folder_path, 'r') as file:
            filecontent = list(csv.reader(file, delimiter='\t'))
            self.index_type = filecontent[0][1]
            self.size_filter = int(filecontent[1][1])
            self.stemmer_enabled = True if filecontent[2][1] == 'True' else False
            self.stopwords_path = filecontent[3][1]
            self.use_positions = True if filecontent[4][1] == 'True' else False

    def store_files_to_open(self, terms):
        term_result={}
        result={}
        for term in terms.keys():
            term_size = len(terms[term])
            doc = self.master_index[term]['file_path']
            idf = self.master_index[term]['idf']

            term_result={}
            result={}
            result['idf'] = idf
            result['count'] = term_size
            
            term_result[term] = result
            if doc not in self.files_to_open:
                self.files_to_open[doc] = []

            self.files_to_open[doc].append(term_result)
    
    def read_posting_index_block(self, file_to_analyse):
        with open(file_to_analyse, 'r') as file:
                    filecontent = csv.reader(file, delimiter='\t')
                    for a in filecontent:
                        term = a[0]
                        post = {}
                        for n in range(1, len(a)):
                            values = a[n].split(":")
                            doc_id = values[0]
                            weight = values[1]
                            post[doc_id] = weight
                        self.post_data[term] = post