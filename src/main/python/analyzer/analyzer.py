
import csv
import sys

def loadAndParseData():
    converted_and_concatenated_lists = []
    for filename in sys.argv[1:]:
        f = open(filename, 'rt')
        try:
            reader = csv.reader(f)
            converted_and_concatenated_lists += map(lambda row: map(lambda field: float(field), row), reader)
        finally:
            f.close()

    return converted_and_concatenated_lists

def main():
    input_data = loadAndParseData()

if __name__ == "__main__":
    main()