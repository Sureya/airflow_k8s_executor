from typing import TextIO, List, Protocol


class DisambiguationAdapter(object):
    '''This class makes it possible to type-check the "adapter" argument
    passed to the Disambiguator constructor'''
    def is_destination_name_ambiguous(self) -> bool:
        return True


class Disambiguator(object):
    '''Defines an interface for checking if a travel destination in input file
    has a non-unique name. London, Canada and London, UK are good examples.'''

    def __init__(adapter: DisambiguationAdapter) -> None:
        self.adapter = adapter

    def disambiguate(input_file: TextIO) -> List[bool]:
        return [self.adapter.is_destination_name_ambiguous(row) for row in input_file]
