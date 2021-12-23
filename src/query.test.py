from filecmp import cmp
from query import Query

# # nonpositional bm25 unit test
# dataset_name = 'index/unitTestFiles/bm25/nonpositional/'
# query = Query(data_path=dataset_name)
# query.process_query('beatriz diana')
# query.process_query('beatriz alfredo')
# new_file = dataset_name + 'query_result.txt'
# reference_file = dataset_name + 'query_result_reference.txt'
# assert cmp(new_file, reference_file, shallow=False)

# # positional bm25 unit test
# dataset_name = 'index/unitTestFiles/bm25/positional/'
# query = Query(data_path=dataset_name)
# query.process_query('beatriz diana')
# query.process_query('beatriz alfredo')
# new_file = dataset_name + 'query_result.txt'
# reference_file = dataset_name + 'query_result_reference.txt'
# assert cmp(new_file, reference_file, shallow=False)

# nonpositional lnc.ltc unit test
dataset_name = 'index/unitTestFiles/lnc.ltc/nonpositional/'
query = Query(data_path=dataset_name)
query.process_query('beatriz maria')
query.process_query('beatriz alfredo')
new_file = dataset_name + 'query_result.txt'
reference_file = dataset_name + 'query_result_reference.txt'
# # assert cmp(new_file, reference_file, shallow=False)

# # positional lnc.ltc unit test
# dataset_name = 'index/unitTestFiles/lnc.ltc/positional/'
# query = Query(data_path=dataset_name)
# query.process_query('beatriz diana')
# query.process_query('beatriz alfredo')
# new_file = dataset_name + 'query_result.txt'
# reference_file = dataset_name + 'query_result_reference.txt'
# assert cmp(new_file, reference_file, shallow=False)
