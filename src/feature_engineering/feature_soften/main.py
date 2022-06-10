from fe_soften import soften
import argparse
from digitforce.aip.common.logging_config import setup_console_log


def main():
    setup_console_log()
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_file', type=str)
    parser.add_argument('--soften_method', type=str)
    parser.add_argument('--sep', type=str, help='the separator of columns')
    parser.add_argument('--cols', type=str, help='the columns to soften')
    parser.add_argument('--thresh_max', type=int)
    parser.add_argument('--thresh_min', type=int)
    parser.add_argument('--percent_max', type=float)
    parser.add_argument('--percent_min', type=float)
    args = parser.parse_args()

    input_file = args.input_file
    soften_method = args.soften_method
    sep = args.sep
    cols = args.cols.split(',')

    thresh_max = thresh_min = percent_max = percent_min = None
    if soften_method == 'thresh':
        if not args.thresh_max or not args.thresh_min:
            raise ValueError('thresh_max and thresh_min must be required')
        thresh_max = args.thresh_max
        thresh_min = args.thresh_min

    elif soften_method == 'percent':
        if not args.percent_max or not args.percent_min:
            raise ValueError('percent_max and percent_min must be required')
        percent_max = args.percent_max
        percent_min = args.percent_min
    soften(input_file, sep, soften_method, cols, thresh_max, thresh_min, percent_max, percent_min)


if __name__ == '__main__':
    main()
