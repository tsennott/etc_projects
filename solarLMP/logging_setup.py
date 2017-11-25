import logging
import sys


def logging_setup(print_level,logfile):
    log = logging.getLogger("main")
    # Clear any previous handlers
    if log.handlers:
        log.handlers = []
    #Setup
    log.setLevel(logging.DEBUG)
    fh = logging.FileHandler(logfile)
    formatter = logging.Formatter('%(asctime)s - %(name)s - (%(levelname)-6s) - (%(threadName)-10s) - %(message)s')
    fh.setFormatter(formatter)
    if print_level==1:
        ste = logging.StreamHandler()
        ste.setLevel(logging.ERROR)
        log.addHandler(ste)
    if print_level==2:
        sto = logging.StreamHandler(sys.stdout)
        sto.setLevel(logging.INFO)
        log.addHandler(sto)
    if print_level==3:
        sto = logging.StreamHandler(sys.stdout)
        sto.setLevel(logging.DEBUG)
        log.addHandler(sto)
    log.addHandler(fh)