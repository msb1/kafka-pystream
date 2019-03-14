'''
Created on Feb 26, 2019

@author: Barnwaldo
'''
import functools

def chain(func):
    @functools.wraps(func)
    def wrapped(*args, **kwargs):
        # Assume it's a method.
        self = args[0]
        func(*args, **kwargs)
        return self
    return wrapped


