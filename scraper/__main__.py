import sys

if __name__ == "__main__":
    # check for first arg (python3 scraper tenor)
    source = None
    if len(sys.argv) > 1:
        # set the source to the first arg
        source = sys.argv[1]

    from scraper import main
    main(source)