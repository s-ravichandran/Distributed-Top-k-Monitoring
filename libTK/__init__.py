###################################################################
# Copyright 2013-2014 All Rights Reserved
# Authors: The Paradrop Team
###################################################################

"""
    Primary entry point into the ParaDrop framework.
"""

import sys, time, urllib, json
import os as origOS
import traceback

__all__ = ['out', 'verbose', 'timeflt', 'timeint', 'timestr', 'logPrefix', 'Colors', 'Output', 'convertUnicode', 'Stdout', 'Stderr', 'urlEncodeMe', 'urlDecodeMe', 'json2str', 'str2json']

timeflt = lambda: time.time()
timeint = lambda: int(time.time())
timestr = lambda x=None: time.asctime(time.localtime(x)) if x else time.asctime()
verbose = False

def install(theOut):
    """Install a new out module."""
    __builtins__.out = theOut


def logPrefix(*args):
    """Setup a default logPrefix for any function that doesn't overwrite it."""
    # Who called us?
    funcName = sys._getframe(1).f_code.co_name
    modName = origOS.path.basename(sys._getframe(1).f_code.co_filename).split('.')[0].upper()
    if(verbose):
        line = "(%d)" % sys._getframe(1).f_lineno
    else:
        line = ""
    
    if(args):
        return '[%s.%s%s @ %s %s]' % (modName, funcName, line, timestr(), ', '.join([str(a) for a in args]))
    else:
        return '[%s.%s%s @ %s]' % (modName, funcName, line, timestr())

def convertUnicode(elem):
    """Converts all unicode strings back into UTF-8 (str) so everything works.
        Call this function like:
            json.loads(s, object_hook=convertUnicode)"""
    if isinstance(elem, dict):
        return {convertUnicode(key): convertUnicode(value) for key, value in elem.iteritems()}
    elif isinstance(elem, list):
        return [convertUnicode(element) for element in elem]
    elif isinstance(elem, unicode):
        return elem.encode('utf-8')
    #DFW: Not sure if this has to be here, but deal with possible "null" MySQL strings
    elif(elem == 'null'):
        return None
    else:
        return elem

def urlEncodeMe(elem, safe=' '):
    """
        Converts any values that would cause JSON parsing to fail into URL percent encoding equivalents.
        This function can be used for any valid JSON type including str, dict, list.
        Returns:
            Same element properly encoded.
    """
    # What type am I?
    if isinstance(elem, dict):
        return {urlEncodeMe(key, safe): urlEncodeMe(value, safe) for key, value in elem.iteritems()}
    elif isinstance(elem, list):
        return [urlEncodeMe(element, safe) for element in elem]
    elif isinstance(elem, str):
        # Leave spaces alone, they are save to travel for JSON parsing
        return urllib.quote(elem, safe)
    else:
        return elem

def urlDecodeMe(elem):
    """
        Converts any values that would cause JSON parsing to fail into URL percent encoding equivalents.
        This function can be used for any valid JSON type including str, dict, list.
        Returns:
            Same element properly decoded.
    """
    # What type am I?
    if isinstance(elem, dict):
        return {urlDecodeMe(key): urlDecodeMe(value) for key, value in elem.iteritems()}
    elif isinstance(elem, list):
        return [urlDecodeMe(element) for element in elem]
    elif isinstance(elem, str):
        # Leave spaces alone, they are save to travel for JSON parsing
        return urllib.unquote(elem)
    else:
        return elem

def json2str(j, safe=' '):
    """
        Properly converts and encodes all data related to the JSON object into a string format
        that can be transmitted through a network and stored properly in a database.
        Arguments:
            @j    : json to be converted
            @safe : optional, string of chars to pass to urlEncodeMe that are declared safe (don't encode)
    """
    return json.dumps(urlEncodeMe(j, safe), separators=(',', ':'))

def str2json(s):
    t = json.loads(s, object_hook=convertUnicode)
    # If t is a list, object_hook was never called (by design of json.loads)
    # deal with that situation here
    if(isinstance(t, list)):
        t = [convertUnicode(i) for i in t]
    # Make sure to still decode any strings
    return urlDecodeMe(t)

def printme(*args):
    funcName = sys._getframe(1).f_code.co_name
    if(args and type(args) is tuple and len(args) == 1):
        s = "~~ %s '%s'" % (funcName, args[0])
    else:
        s = "~~ %s " % funcName + str(args)
    print(s)

class Colors:
    # Regular ANSI supported colors foreground
    BLACK = '\033[30m'
    RED = '\033[31m'
    GREEN = '\033[32m'
    YELLOW = '\033[33m'
    BLUE = '\033[34m'
    MAGENTA = '\033[35m'
    CYAN = '\033[36m'
    WHITE = '\033[37m'
    
    # Regular ANSI suported colors background
    BG_BLACK = '\033[40m'
    BG_RED = '\033[41m'
    BG_GREEN = '\033[42m'
    BG_YELLOW = '\033[43m'
    BG_BLUE = '\033[44m'
    BG_MAGENTA = '\033[45m'
    BG_CYAN = '\033[46m'
    BG_WHITE = '\033[47m'

    # Other abilities
    BOLD = '\033[1m'
    
    # Ending sequence
    END = '\033[0m'
    
    # Color suggestions
    HEADER = BLUE
    VERBOSE = BLACK
    INFO = GREEN
    PERF = WHITE
    WARN = YELLOW
    ERR = RED
    SECURITY = BOLD + RED
    FATAL = BG_WHITE + RED

class IOutput:
    """Interface class that all Output classes should inherit."""
    def __call__(self, args):
        pass
    def __repr__(self):
        return "REPR"

class Fileout(IOutput):
    def __init__(self, filename, truncate=False):
        self.filename = filename
        self.mode = None
        if(truncate):
            self.mode = "w"
        else:
            self.mode = "a"
    def __call__(self, args):
        try:
            fd = open(self.filename, self.mode)
            # Make sure args is a str type
            if(not isinstance(args, str)):
                args = str(args)
            fd.write(args)
            fd.flush()
            fd.close()
        except:
            pass

class Stdout(IOutput):
    def __init__(self, color=None, other_out_types=None):
        self.color = color
        if(other_out_types and type(other_out_types) is not list):
            other_out_types = [other_out_types]
        self.other_out = other_out_types
    
    def __call__(self, args):
        # Make sure args is a str type
        if(not isinstance(args, str)):
            args = str(args) 
        msg = ""
        if(self.color):
            msg = self.color + args + Colors.END
        else:
            msg = args
        sys.stdout.write(msg)
        sys.stdout.flush()
        if self.other_out:
            for item in self.other_out:
                obj = item 
                obj(args)

class Stderr(IOutput):
    def __init__(self, color=None, other_out_types=None):
        self.color = color
        if(other_out_types and type(other_out_types) is not list):
            other_out_types = [other_out_types]
        self.other_out = other_out_types
    
    def __call__(self, args):
        # Make sure args is a str type
        if(not isinstance(args, str)):
            args = str(args)
        msg = ""
        if(self.color):
            msg = self.color + args + Colors.END
        else:
            msg = args
        sys.stderr.write(msg)
        sys.stderr.flush()
        if self.other_out:
            for item in self.other_out:
                obj = item 
                obj(args)

class FakeOutput(IOutput):
    def __call__(self, args):
        pass

class Output():
    """
        Class that allows stdout/stderr trickery.
        By default the paradrop object will contain an @out variable
        (defined below) and it will contain 2 members of "err" and "fatal".

        Each attribute of this class should be a function which points
        to a class that inherits IOutput(). We call these functions
        "output streams".

        The way this Output class is setup is that you pass it a series
        of kwargs like (stuff=OutputClass()). Then at any point in your
        program you can call "paradrop.out.stuff('This is a string\n')".

        This way we can easily support different levels of verbosity without
        the need to use some kind of bitmask or anything else.
        Literally you can define any kind of output call you want (paradrop.out.foobar())
        but if the parent script doesn't define the kwarg for foobar then the function
        call just gets thrown away.
        
        This is done by the __getattr__ function below, basically in __init__ we set
        any attributes you pass as args, and anything else not defined gets sent to __getattr__
        so that it doesn't error out.

        
        Currently these are the choices for Output classes:
            - StdoutOutput() : output sent to sys.stdout
            - StderrOutput() : output sent to sys.stderr
            - FileOutput()   : output sent to filename provided
    """
            
    def __init__(self, **kwargs):
        """Setup the initial set of output stream functions."""
        for name, func in kwargs.iteritems():
            setattr(self, name, func)
    
    def __getattr__(self, name):
        """Catch attribute access attempts that were not defined in __init__
            by default throw them out."""
        return FakeOutput()

    def __setattr__(self, name, val):
        """Allow the program to add new output streams on the fly."""
        if(verbose):
            print('>> Adding new Output stream %s' % name)
        # WARNING you cannot call setattr() here, it would recursively call back into this function
        self.__dict__[name] = val
    def __repr__(self):
        return "REPR"

# Create a standard out module to be used if no one overrides it
out = Output(
            header=Stdout(Colors.HEADER),
            verbose=FakeOutput(),
            info=Stdout(Colors.INFO),
            perf=Stdout(Colors.PERF),
            warn=Stdout(Colors.WARN),
            err=Stderr(Colors.ERR),
            security=Stderr(Colors.SECURITY),
            fatal=Stderr(Colors.FATAL)
            )
