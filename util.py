#!/usr/bin/env python3

import functools
from lxml.html.clean import Cleaner
from IPython.display import display_html, IFrame
from urllib.parse import quote

def display_iframe(html, width, height):
    src = 'data:text/html;charset=utf-8,' + quote(html)
    return IFrame(src=src, width=width, height=height)

def display_raw(html):
    cleaner = Cleaner()
    cleaner.javascript = True
    cleaner.style = True
    html = cleaner.clean_html(html)
    return display_html(html, raw=True)

def none_on_error(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception:
            return None
    return wrapper

def main():
    pass

if __name__ == '__main__':
    main()
