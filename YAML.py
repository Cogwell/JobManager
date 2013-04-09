import sys, yaml
import argparse, textwrap
#apt-get install python-yaml

def load(filePath):
    """ Borrowed from Brian Clowers -- used to load yaml configuration file. """
    try:
        with open(filePath, 'r') as f:
            return yaml.load(f)
    except:
        sys.exit("Error loading YAML configuration file at '%s'." % filePath)

def write(data, filePath):
    try:
        with open(filePath, 'w') as f:
            yaml.dump(data, f, default_flow_style=False)
    except:
        sys.exit("Error loading YAML configuration file at '%s'." % filePath)

def merge(dict1, dict2):
    d1Keys = set(dict1.keys())
    d2Keys = set(dict2.keys())

    if len(d1Keys.intersection(d2Keys)) > 0:
        print "Dictionaries intersect."
        return
    dict1.update(dict2)
    del dict1

if __name__ == "__main__":
    desc = '''\
           YAML Merger.'''
    parser = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter,
                                description=textwrap.dedent(desc))
    parser.add_argument('file1', type=str, help="Path to yaml config.")
    parser.add_argument('file2', type=str, help="Path to yaml config.")
    parser.add_argument('-name', type=str, default="merged.yaml", help="Name of merged yaml file.")
    args = parser.parse_args()

    d1 = load(args.file1)
    d2 = load(args.file2)

    merge(d1, d2) # now d1 has all values

    write(d1, args.name)
