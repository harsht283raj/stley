import argparse
import csv
import errno
import os
from typing import Dict, Callable, Any, Optional, List

from lxml import etree
import pandas as pd
from pathlib import Path



class XMLParser(object):

    def __init__(self,
                 xml_file: str,
                 python_callable: Callable[[etree.Element, Any], None],
                 callable_args: Optional[List] = None,
                 callable_kwargs: Optional[Dict] = None,
                 tag: Optional[str] = None,
                 dtd_validation: bool = False,
                 schema: Optional[bytes] = None) -> None:

        if not callable(python_callable):
            raise TypeError('The `python_callable` parameter must be callable.')

        self.xml_file = xml_file
        self.python_callable = python_callable
        self.callable_args = callable_args or []
        self.callable_kwargs = callable_kwargs or {}
        self.tag = tag
        self.dtd_validation = dtd_validation
        self.schema = etree.XMLSchema(etree.XML(schema)) if schema else None

        if self.is_non_empty_file(self.xml_file):
            xml_tree = etree.iterparse(
                self.xml_file,
                tag=self.tag,
                dtd_validation=self.dtd_validation,
                events=('start-ns', 'end'),  
                remove_blank_text=True,
                encoding='utf-8',
                schema=self.schema
            )
            self.fast_iteration(xml_tree)  
        else:
            raise RuntimeError(f'{self.xml_file} is empty or non-existing.')

    def fast_iteration(self, xml_tree: etree.iterparse) -> None:
        namespaces = {}

        for event, element in xml_tree:

            if event == 'start-ns':  
                prefix, url = element
                if not prefix:
                    prefix = 'ns'
                namespaces[prefix] = url  
            elif event == 'end': 
                if namespaces:
                    self.callable_kwargs.update({'namespaces': namespaces})

                self.python_callable(element, *self.callable_args, **self.callable_kwargs)
                element.clear()
                
                for ancestor in element.xpath('ancestor-or-self::*'):
                    while ancestor.getprevious() is not None:
                        del ancestor.getparent()[0]

        del xml_tree

    @staticmethod
    def is_non_empty_file(file: str) -> bool:
        return os.path.isfile(file) and os.path.getsize(file) > 0

    @staticmethod
    def delete_file(file: str) -> None:
        try:
            os.remove(file)
            print(f'File deleted: {file}.')
        except OSError as os_error:
            if os_error.errno != errno.ENOENT:
                print(f'{str(os_error)}.')
