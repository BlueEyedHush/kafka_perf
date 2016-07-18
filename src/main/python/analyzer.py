import ast
import glob
import numpy as np
import sys


def load_and_parse_data(results_root, topics_list, series, nodes_count):
    results = {} # map (from topics_no) to list of lists (series of results from different threads/nodes)
    warns = {}
    for topics_no in topics_list:
        results[topics_no] = []
        found_files = 0

        for series_no in range(0, series):
            result_files = glob.glob('{}/t{}_{}/*/results'.format(results_root, topics_no, series_no))
            found_files += len(result_files)
            converted_and_concatenated_lists = []

            for filename in result_files:
                f = open(filename, 'rt')
                converted_and_concatenated_lists += map(lambda line: float(line), f)
                f.close()

            results[topics_no].append(converted_and_concatenated_lists)

        expected = nodes_count*series
        if found_files < expected:
            warns[topics_no] = '{}/{}'.format(found_files, expected)
        else:
            warns[topics_no] = 'OK'

    return (results, warns)

def sum_across_threads_and_nodes(data):
    def sum_list_of_lists_element(list_of_lists):
        return map(lambda list: sum(list), list_of_lists)
    return dict(map(lambda (k,v): (k, sum_list_of_lists_element(v)), data.iteritems()))

def filter_missing(data):
    return dict(map(lambda (k,v): (k, filter(lambda lv: lv > 0.0, v)),data.iteritems()))

def calculate_percentiles(data):
    map_with_numpy_arr = dict(map(lambda (k,v): (k, np.asarray(v)), data.iteritems()))
    map_of_percentiles = dict(map(lambda (k,v): (k, np.percentile(v, 10)), map_with_numpy_arr.iteritems()))
    return map_of_percentiles

def calculate_stats(messages_in_time, time, message_size):
    messages_per_sec = messages_in_time / time
    throughput = float(messages_in_time) * message_size / time
    return (messages_per_sec, throughput)

def print_results(map, warns, time, msize):
    for k in sorted(map):
        mesgs_in_time = map[k]
        mesgs_per_sec, throughput = calculate_stats(mesgs_in_time, time, msize)
        print('{: >10} {: >20,.2f} {: >20,.2f} {: >20,.2f} {: >20}'.format(k, mesgs_in_time, mesgs_per_sec, throughput, warns[k]))

def main():
    results_root = sys.argv[1]
    topics_list = ast.literal_eval(sys.argv[2])
    series = int(sys.argv[3])
    mszie = int(sys.argv[4])
    time= float(sys.argv[5])
    nodes_count = int(sys.argv[6])

    input_data, warns = load_and_parse_data(results_root, topics_list, series, nodes_count)
    summed_input_data = sum_across_threads_and_nodes(input_data)
    without_missing = filter_missing(summed_input_data)
    map_of_percentiels = calculate_percentiles(without_missing)

    print_results(map_of_percentiels, warns, time, mszie)

if __name__ == "__main__":
    main()