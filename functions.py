
import os
import pandas as pd
import time

class path_error(Exception):
    pass

def check_file_path(file_name):
    """
        check_file_path(file_name)
        This function checks the existance of a file 'file_name'
    """
    if not os.path.exists(file_name):
        raise path_error('\n"{}"  doesn\'t exist \nplease, check its \
existence with all input data'.format(file_name))


def check_dir_path(dir_name):
    """
        check_dir_path(dir_name)
        This function checks the existance of a directory 'dir_name'
    """
    if not os.path.exists(dir_name):
        raise path_error('\n"{}"  doesn\'t exist \\nplease, check its \
existence with all input data'.format(dir_name))


def print_data_frame(data_frame):
    """
        print_data_frame(data_frame)
        This function prints the head of 'data_frame'
    """
    print(data_frame.__name)
    print(data_frame.info())
    display(data_frame.head())




    
    
