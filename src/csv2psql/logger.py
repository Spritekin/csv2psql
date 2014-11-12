import logging
import json

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

__handler = logging.FileHandler('csv2psql.log')
__handler.setLevel(logging.INFO)

__formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
__handler.setFormatter(__formatter)

logger.addHandler(__handler)


def _swallow(fn):
    try:
        fn()
    except Exception as e:
        _error = e
        # ignore TDOD figure out where error is happening


def info(do_print, msg, *args, **kwargs):
    """
    See log_print
    """
    fn = lambda: logger.info(msg, *args, **kwargs)

    _swallow(log_print(do_print, msg, fn, *args, **kwargs))


def debug(do_print, msg, *args, **kwargs):
    """
    See log_print
    """
    fn = lambda: logger.debug(msg, *args, **kwargs)
    _swallow(log_print(do_print, msg, fn, *args, **kwargs))


def warning(do_print, msg, *args, **kwargs):
    """
    See log_print
    """
    fn = lambda: logger.warning(msg, *args, **kwargs)
    _swallow(log_print(do_print, msg, fn, *args, **kwargs))


def error(do_print, msg, *args, **kwargs):
    """
    See log_print
    """
    fn = lambda: logger.error(msg, *args, **kwargs)
    _swallow(log_print(do_print, msg, fn, *args, **kwargs))


def critical(do_print, msg, *args, **kwargs):
    """
    See log_print
    """
    fn = lambda: logger.critical(msg, *args, **kwargs)
    _swallow(log_print(do_print, msg, fn, *args, **kwargs))


def log_print(do_print, msg, fn, *args, **kwargs):
    """
    Encapsulate monotonous print and logging (sometimes you want todo both).
    One for outfile and other for logfile
    :param msg: basic message
    :param do_print: do we print as well
    :param fn: lambda to call correct logger level function
    :param args: Splat array of args to print
    :param kwargs: Dict args to print as json
    :return:
    """
    fn()
    if do_print:
        sql_comment = "-- %s"

        print sql_comment % msg

        for a in args:
            print sql_comment % a

        print sql_comment % json.dumps(kwargs)

