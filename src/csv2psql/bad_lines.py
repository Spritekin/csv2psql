from __future__ import print_function
import fileinput

# http://stackoverflow.com/questions/17747522/how-to-delete-a-line-from-a-text-file-using-the-line-number-in-python
def remove_bad_line_number(line_number, filename):
    fn = lambda: fileinput.lineno() == line_number
    remove_bad_line(filename, fn)


def remove_bad_line_phrase(phrase, filename):
    fn = lambda line: phrase in line
    remove_bad_line(filename, fn)


def remove_bad_line(filename, fn):
    for line in fileinput.input(filename, inplace=True):
        if fn(line):
            continue
        print(line, end='')
