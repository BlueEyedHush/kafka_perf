
import sys
import numpy as np
import ast
import glob
import pprint

def load_and_parse_data(results_root, topics_list, series):
    results = {} # map (from topics_no) to list of lists (series of results from different threads/nodes)
    for topics_no in topics_list:
        results[topics_no] = []
        for series_no in range(0, series):
            result_files = glob.glob('{}/t{}_{}/*/results'.format(results_root, topics_no, series_no))
            converted_and_concatenated_lists = []

            for filename in result_files:
                f = open(filename, 'rt')
                converted_and_concatenated_lists += map(lambda line: float(line), f)
                f.close()

            results[topics_no].append(converted_and_concatenated_lists)

    return results

def sum_across_threads_and_nodes(data):
    def sum_list_of_lists_element(list_of_lists):
        return map(lambda list: sum(list), list_of_lists)
    return dict(map(lambda (k,v): (k, sum_list_of_lists_element(v)), data.iteritems()))

def calculate_percentiles(data):
    map_with_numpy_arr = dict(map(lambda (k,v): (k, np.asarray(v)), data.iteritems()))
    map_of_percentiles = dict(map(lambda (k,v): (k, np.percentile(v, 10)), map_with_numpy_arr.iteritems()))
    return map_of_percentiles

def combine_percentiles(perc):
    return np.sum(perc)

def print_results(map):
    for k in sorted(map):
        print('{: >10} {: >20,.2f}'.format(k,map[k]))

def main():
    results_root = sys.argv[1]
    topics_list = ast.literal_eval(sys.argv[2])
    series = int(sys.argv[3])

    input_data = load_and_parse_data(results_root, topics_list, series)
    summed_input_data = sum_across_threads_and_nodes(input_data)
    map_of_percentiels = calculate_percentiles(summed_input_data)

    print_results(map_of_percentiels)
    # pp = pprint.PrettyPrinter(indent=2)
    # print pp.pprint(map_of_percentiels)
    #print combine_percentiles(percentiels)

if __name__ == "__main__":
    main()