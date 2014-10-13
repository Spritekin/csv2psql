from __future__ import print_function
import fileinput
import logger
from os import popen


# http://stackoverflow.com/questions/17747522/how-to-delete-a-line-from-a-text-file-using-the-line-number-in-python
def remove_bad_line_number(line_number, filename):
    # fn = lambda line, cur_number: cur_number == line_number
    # remove_bad_line(filename, fn)
    cmd = "sed -ie '{linenumber}d' {filename}".format(
        filename=filename, linenumber=line_number)
    popen(cmd).read()


def remove_bad_line_phrase(phrase, filename):
    fn = lambda line, cur_number: phrase in line
    remove_bad_line(filename, fn)


def remove_bad_line(filename, fn):
    for line in fileinput.input(filename, inplace=1):
        cur_line_number = fileinput.lineno()
        if fn(line, cur_line_number):
            logger.error(False, "Skipping line in file %s" % filename)
            logger.error(False, "Skipping line %s" % cur_line_number)
            continue
        else:
            print(line, end='')