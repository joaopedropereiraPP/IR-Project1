from filecmp import cmp
from query import Query

# nonpositional bm25 unit test
# dataset_name = 'index/unitTestFiles/bm25/nonpositional/'
# dataset_name = 'index/data1'
# query = Query(data_path=dataset_name)
# query.process_query('beatriz diana')
# query.process_query('beatriz alfredo')
# new_file = dataset_name + 'query_result.txt'
# reference_file = dataset_name + 'query_result_reference.txt'
# assert cmp(new_file, reference_file, shallow=False)


# print(query.read_silver_standard_file('queries/queries.relevance.txt'))
# query.read_silver_standard_file('queries/queries.relevance.data1.txt')
# print(query.evaluate_system('queries/queries.relevance.data1.txt'))

# dataset_name = 'index/data1'
# query = Query(data_path=dataset_name)
# positions_list = [
#     (3, 'isto'),
#     (5, 'aquilo'),
#     (10, 'esta'),
#     (12, 'este'),
#     (13, 'aqueloutro'),
#     (18, 'aquele'),
# ]
# positions_list = [
#     (3, 'isto'),
#     (10, 'aquilo'),
#     (17, 'esta'),
#     (19, 'este'),
# ]
# print(query.calculate_positional_boost(positions_list))

# dataset_name = 'index/amazon_reviews_us_Digital_Music_Purchase_v1_00'
# query = Query(data_path=dataset_name, dump_results_file=False)
# query_result, total_time = query.process_query('greatest rock album')
# print(query_result[:11])

dataset_name = 'index/amazon_reviews_us_Digital_Music_Purchase_v1_00'
query = Query(data_path=dataset_name, dump_results_file=False, positional_boost_enabled=True, span_size=20)
print(query.evaluate_system('queries/queries.relevance.txt'))

# CHANGE TO PERFORM 10 QUERIES AGAIN WHEN EVALUATING!!


# # positional bm25 unit test
# dataset_name = 'index/unitTestFiles/bm25/positional/'
# query = Query(data_path=dataset_name)
# query.process_query('beatriz diana')
# query.process_query('beatriz alfredo')
# new_file = dataset_name + 'query_result.txt'
# reference_file = dataset_name + 'query_result_reference.txt'
# # assert cmp(new_file, reference_file, shallow=False)

# # nonpositional lnc.ltc unit test
# dataset_name = 'index/unitTestFiles/lnc.ltc/nonpositional/'
# query = Query(data_path=dataset_name)
# query.process_query('beatriz maria')
# query.process_query('beatriz alfredo')
# new_file = dataset_name + 'query_result.txt'
# reference_file = dataset_name + 'query_result_reference.txt'
# # # assert cmp(new_file, reference_file, shallow=False)

# # positional lnc.ltc unit test
# dataset_name = 'index/unitTestFiles/lnc.ltc/positional/'
# query = Query(data_path=dataset_name)
# query.process_query('beatriz diana')
# query.process_query('beatriz alfredo')
# new_file = dataset_name + 'query_result.txt'
# reference_file = dataset_name + 'query_result_reference.txt'
# # assert cmp(new_file, reference_file, shallow=False)
