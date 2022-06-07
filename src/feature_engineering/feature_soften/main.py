from fe_soften import soften


def main():

    import sys
    df = sys.argv[1]
    soften_method = sys.argv[2]
    cols = sys.argv[3].split(',')
    thresh_max = thresh_min = percent_max = percent_min = None
    if soften_method == 'thresh':
        if not sys.argv[4] or not sys.argv[5]:
            raise ValueError('thresh_max and thresh_min must be required')
        thresh_max = sys.argv[4]
        thresh_min = sys.argv[5]

    elif soften_method == 'percent':
        if not sys.argv[4] or not sys.argv[5]:
            raise ValueError('percent_max and percent_min must be required')
        percent_max = sys.argv[4]
        percent_min = sys.argv[5]
    soften(df, soften_method, cols, thresh_max, thresh_min, percent_max, percent_min)


if __name__ == '__main__':
    main()
