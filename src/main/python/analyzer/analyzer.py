
import csv
import sys
import numpy as np

def load_and_parse_data():
    converted_and_concatenated_lists = []
    for filename in sys.argv[1:]:
        f = open(filename, 'rt')
        try:
            reader = csv.reader(f)
            converted_and_concatenated_lists += map(lambda row: map(lambda field: float(field), row), reader)
        finally:
            f.close()

    return converted_and_concatenated_lists

def calculate_percentiles(data):
    numpy_arrays = map(lambda list: np.asarray(list), data)
    return map(lambda arr: np.percentile(arr, 10), numpy_arrays)

def combine_percentiles(perc):
    return np.sum(perc)/len(perc)

def main():
    input_data = load_and_parse_data()
    percentiels = calculate_percentiles(input_data)
    print combine_percentiles(percentiels)

if __name__ == "__main__":
    main()