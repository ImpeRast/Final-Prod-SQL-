import os
import shutil
import pathlib
import re

x = os.getcwd()
y = os.listdir()
print(x)
print()

def num_of_starts(file_list):
    """ need to programm logic (for if conditiosn), count the num of files to load"""
    if pattern.match(filename):
        file_list.append(filename)
    if len(file_list) == 9:
        counter = 1
    if len(file_list) == 6:
        counter = 2
    if len(file_list) == 3:
        counter = 3
    else:
        print('not anouth files to load!')
        counter = 'Error'
    return counter
    print(counter)

def find_and_sort_files():
    """ find passport_black_list/ transactions/ terminals in current dict and sort them"""
    ls_of_passport_bl = []
    ls_of_terminals = []
    ls_of_transactions = []
    pattern1 = re.compile(r'^(passport_blacklist)_\d{8}.(xlsx|txt)$')
    pattern2 = re.compile(r'^(transactions)_\d{8}.(xlsx|txt)$')
    pattern3 = re.compile(r'^(terminals)_\d{8}.(xlsx|txt)$')
    pattern_list = [pattern1, pattern2, pattern3]
    for filename in os.listdir(os.getcwd()):
        for pattern in pattern_list:
            if  pattern == pattern1 and pattern.match(filename):
                ls_of_passport_bl.append(os.path.abspath(filename))
                ls_of_passport_bl.sort()
            if  pattern == pattern2 and pattern.match(filename):
                ls_of_transactions.append(os.path.abspath(filename))
                ls_of_transactions.sort()
            if pattern == pattern3 and pattern.match(filename):
                ls_of_terminals.append(os.path.abspath(filename))
                ls_of_terminals.sort()
    return ls_of_passport_bl, ls_of_transactions, ls_of_terminals


print(find_and_sort_files())

pas_file, trans_file, term_file = find_and_sort_files()
print(type(pas_file[1]))
