import json
import subprocess
from threading import Thread
import numpy as np
import pandas as pd
import cv2
import PIL
import requests
import uuid
import os
import sys
import traceback
import inspect
import re
import base64
import yaml
import zlib
import hashlib
import select

from subprocess import Popen

from naeural_core.utils.thread_raise import ctype_async_raise

try:
  # Temporarily guard the bs4 import until we can be sure
  # that it's available in all environments.
  import bs4
except ImportError as _:
  bs4 = None


from collections import OrderedDict, defaultdict, deque
from io import BufferedReader, BytesIO
from time import sleep, time
from datetime import datetime, timedelta, timezone
from copy import deepcopy
from xml.etree import ElementTree
from urllib.parse import urlparse, urlunparse
from functools import partial

from naeural_core import constants as ct

from naeural_core.serving.ai_engines.utils import (
  get_serving_process_given_ai_engine,
  get_ai_engine_given_serving_process,
  get_params_given_ai_engine
)

from naeural_core.utils.plugins_base.persistence_serialization_mixin import _PersistenceSerializationMixin
from naeural_core.utils.system_shared_memory import NumpySharedMemory

from naeural_core.main.ver import __VER__ as core_version    
from ratio1._ver import __VER__ as sdk_version   


GIT_IGNORE_AUTH = ["-c","http.https://github.com/.extraheader="]

class NestedDotDict(dict):
  # TODO: maybe use https://github.com/mewwts/addict/blob/master/addict/addict.py
  __getattr__ = defaultdict.__getitem__
  __setattr__ = defaultdict.__setitem__
  __delattr__ = defaultdict.__delitem__

  def __init__(self, *args, **kwargs):
    super(NestedDotDict, self).__init__(*args, **kwargs)
    for key, value in self.items():
      if isinstance(value, dict):
        self[key] = NestedDotDict(value)
      elif isinstance(value, (list, tuple)):
        self[key] = type(value)(
          NestedDotDict(v) if isinstance(v, dict) else v for v in value
        )        
              
  def __deepcopy__(self, memo):
    return NestedDotDict({k: deepcopy(v, memo) for k, v in self.items()})
                
  def __reduce__(self):
    return (self.__class__, (), self.__getstate__())

  def __getstate__(self, obj=None):
    state = {}
    obj = obj or self
    for key, value in obj.items():
      if isinstance(value, NestedDotDict):
        state[key] = self.__getstate__(value)
      else:
        state[key] = value
    return state

  def __setstate__(self, state):
    self.update(state)
  
class DefaultDotDict(defaultdict):
  __getattr__ = defaultdict.__getitem__
  __setattr__ = defaultdict.__setitem__
  __delattr__ = defaultdict.__delitem__
  

class NestedDefaultDotDict(defaultdict):
  """
  A dictionary-like object supporting auto-creation of nested dictionaries and default values for undefined keys.
  """
  def __init__(self, *args, **kwargs):
    super(NestedDefaultDotDict, self).__init__(NestedDefaultDotDict, *args, **kwargs)
    for key, value in dict(self).items():
      if isinstance(value, dict):
        self[key] = NestedDefaultDotDict(value)
      elif isinstance(value, (list, tuple)):
        self[key] = type(value)(
          NestedDefaultDotDict(v) if isinstance(v, dict) else v for v in value
        )

  def __getattr__(self, item):
    if item in self:
      return self[item]
    return self.__missing__(item)

  def __setattr__(self, key, value):
    if isinstance(value, dict) and not isinstance(value, NestedDefaultDotDict):
      value = NestedDefaultDotDict(value)
    defaultdict.__setitem__(self, key, value)

  def __delattr__(self, item):
    try:
      defaultdict.__delitem__(self, item)
    except KeyError as e:
      raise AttributeError(e)

  def __deepcopy__(self, memo):
    return NestedDefaultDotDict({k: deepcopy(v, memo) for k, v in self.items()})

  def __reduce__(self):
    return (self.__class__, (), None, None, iter(self.items()))


class NPJson(json.JSONEncoder):
  """
  Used to help jsonify numpy arrays or lists that contain numpy data types.
  """
  def default(self, obj):
      if isinstance(obj, np.integer):
          return int(obj)
      elif isinstance(obj, np.floating):
          return float(obj)
      elif isinstance(obj, np.ndarray):
          return obj.tolist()
      elif isinstance(obj, np.ndarray):
          return obj.tolist()
      elif isinstance(obj, datetime):
          return obj.strftime("%Y-%m-%d %H:%M:%S")
      else:
          return super(NPJson, self).default(obj)


class LogReader():
  def __init__(self, owner, buff_reader, size=100):
    self.buff_reader: BufferedReader = buff_reader
    self.owner = owner
    self.buf_reader_size = size

    self.buffer = []
    self.done = False
    self.exited = False
    self.thread = None
    # now we start the thread
    self.start()
    return

  def _run(self):
    try:
      while not self.done:
        ready, _, _ = select.select([self.buff_reader], [], [], 0.1)  # Wait up to 0.1s
        if ready:
          text = self.buff_reader.read(self.buf_reader_size)
          if text:  # Check if any data is read
            self.on_text(text)
          else:
            break
        # endif any data ready
    except ct.ForceStopException:
      self.owner.P("Log reader forced to stop.")
    except Exception as exc:
      self.owner.P(f"Log reader exception: {exc}", color='r')
    self.exited = True
    # self.buff_reader.close()
    return

  def on_text(self, text):
    self.buffer.append(text)
    return

  def start(self):
    self.thread = Thread(target=self._run)
    self.thread.start()
    return

  # Public methods
  def stop(self):
    if self.done:
      return
    self.done = True
    self.owner.P("Stopping log reader thread...")
    self.buff_reader.close()
    self.owner.P("Log reader thread should be stopped.")
    if not self.exited:
      self.owner.P("Waiting for log reader thread to stop...")
      self.owner.sleep(0.2)
    # end if

    if not self.exited:
      self.owner.P("Forcing log reader thread to stop...")
      ctype_async_raise(self.thread.ident, ct.ForceStopException)
      self.owner.sleep(0.2)
      self.owner.P("Log reader stopped forcefully.")
    # end if

    self.owner.P("Joining log reader thread...")
    self.thread.join(timeout=0.1)
    self.owner.P("Log reader thread joined.")

    if self.thread.is_alive():
      self.owner.P("Log reader thread is still alive.", color='r')
    else:
      self.owner.P("Log reader thread joined gracefully.")
    # end if

    return

  # TODO: maybe change decode_errors to 'replace' to have something appear in the logs.
  def get_next_characters(self, max_characters=-1, decode='utf-8', decode_errors='ignore'):
    result = []
    
    if max_characters == -1:
      # get all the buffer
      L = len(self.buffer)
      for i in range(L):
        result.append(self.buffer.pop(0))
      # end for
    else:
      L = len(self.buffer)
      nr_chars = 0
      for i in range(L):
        segment = self.buffer[0]

        if nr_chars + len(segment) > max_characters:
          result.append(segment[:max_characters - nr_chars])
          self.buffer[0] = self.buffer[0][max_characters - nr_chars:]
          break
        elif nr_chars + len(segment) == max_characters:
          result.append(segment)
          self.buffer.pop(0)
          break
        else:
          result.append(segment)
          nr_chars += len(segment)
          self.buffer.pop(0)
        # end if
      # end for
    # end if
    result = b''.join(result)
    if decode is None:
      return result
    if decode == False:
      return result
    if decode == True:
      return result.decode('utf-8', errors=decode_errors)
    if len(decode) > 0:
      return result.decode(decode, errors=decode_errors)
    return result

  def get_next_line(self):
    result = []
    L = len(self.buffer)
    for i in range(L):
      segment = self.buffer[i]
      if '\n' in segment:
        pos = segment.index('\n')
        result.append(segment[:pos])
        self.buffer[i] = segment[pos+1:]
        break
      else:
        result.append(segment)
      # end if
    # end for

    if len(result) > 0 and '\n' in result[-1]:
      for _ in range(len(result)):
        self.buffer.pop(0)
      result = ''.join(result)
    else:
      result = None
    # end if

    return result


class _UtilsBaseMixin(
  _PersistenceSerializationMixin
  ):

  def __init__(self):
    super(_UtilsBaseMixin, self).__init__()
    return
  
  
  @property
  def ee_core_ver(self):
    return core_version
  
  @property
  def ee_sdk_ver(self):
    return sdk_version

  
  def trace_info(self):
    """
    Returns a multi-line string with the last exception stacktrace (if any)

    Returns
    -------
    str.

    """
    return traceback.format_exc()
  
  
  def python_version(self):
    """
    Utilitary method for accessing the Python version.
    Returns
    -------
    Version of python
    """
    return sys.version.split()[0]
  
  def get_serving_process_given_ai_engine(self, ai_engine):
    return get_serving_process_given_ai_engine(ai_engine)
  
    

  def timedelta(self, **kwargs):
    """
    Alias of `datetime.timedelta`
    

    Parameters
    ----------
    **kwargs : 
      can contain days, seconds, microseconds, milliseconds, minutes, hours, weeks.


    Returns
    -------
    timedelta object


    Example
    -------
      ```
        diff = self.timedelta(seconds=10)
      ```
    
    """
    return timedelta(**kwargs)  
  
  
  def time(self):
    """
    Returns current timestamp

    Returns
    -------
    time : timestamp (float)
      current timestamp.
      
      
    Example
    -------
      ```
      t1 = self.time()
      ... # do some stuff
      elapsed = self.time() - t1
      ```    

    """

    return time() 

  def now_str(self, nice_print=False, short=False):
    """
    Returns current timestamp in string format
    Parameters
    ----------
    nice_print
    short

    Returns
    -------

    """
    return self.log.now_str(nice_print=nice_print, short=short)

  def get_output_folder(self):
    """
    Provides access to get_output_folder() method from .log
    Returns
    -------

    """
    return self.log.get_output_folder()

  def get_data_folder(self):
    """
    Provides access to get_data_folder() method from .log
    Returns
    -------

    """
    return self.log.get_data_folder()

  def get_logs_folder(self):
    """
    Provides access to get_logs_folder() method from .log
    Returns
    -------

    """
    return self.log.get_logs_folder()

  def get_models_folder(self):
    """
    Provides access to get_models_folder() method from .log
    Returns
    -------

    """
    return self.log.get_models_folder()

  def get_target_folder(self, target):
    """
    Provides access to get_target_folder() method from .log
    Parameters
    ----------
    target

    Returns
    -------

    """
    return self.log.get_target_folder(target)

  def sleep(self, seconds):
    """
    sleeps current job a number of seconds
    """
    sleep(seconds)
    return  


  def uuid(self, size=13):
    """
    Returns a unique id.
  

    Parameters
    ----------
    size : int, optional
      the number of chars in the uid. The default is 13.

    Returns
    -------
    str
      the uid.
      

    Example
    -------
    
      ```
        str_uid = self.uuid()
        result = {'generated' : str_uid}
      ```      

    """
    return str(uuid.uuid4())[:size].replace('-','')
  
  @property
  def json(self):
    """
    Provides access to `json` package

    Returns
    -------
    `json` package      

    """
    return json

  @property
  def yaml(self):
    """
    Provides access to `yaml` package

    Returns
    -------
    `yaml` package      

    """
    return yaml

  @property
  def re(self):
    """
    Provides access to `re` package

    Returns
    -------
    `re` package

    """
    return re
  
  @property
  def inspect(self):
    """
    Provides access to `inspect` package

    Returns
    -------
    `inspect` package      

    """
    return inspect
    
  
  @property
  def requests(self):
    """
    Provides access to `requests` package

    Returns
    -------
    `requests` package      

    """
    return requests

  @property
  def urlparse(self):
    """
    Provides access to `urlparse` method from `urllib.parse` package

    Returns
    -------
    `urlparse` method      

    """
    return urlparse

  @property
  def urlunparse(self):
    """
    Provides access to `urlunparse` method from `urllib.parse` package

    Returns
    -------
    `urlunparse` method      

    """
    return urlunparse
  
  @property
  def consts(self):
    """
    Provides access to E2 constants

    Returns
    -------
    ct : package
      Use `self.consts.CONST_ACME` to acces any required constant

    """
    return ct


  @property
  def const(self):
    """
    Provides access to E2 constants

    Returns
    -------
    ct : package
      Use `self.const.ct.CONST_ACME` to acces any required constant

    """
    return ct

  @property
  def ct(self):
    """
    Provides access to E2 constants

    Returns
    -------
    ct : package
      Use `self.const.ct.CONST_ACME` to acces any required constant

    """
    return ct

  @property
  def ds_consts(self):
    """
    Alias for DatasetBuilder class from E2 constants
    Provides access to constants used in DatasetBuilderMixin
    Returns
    -------
    ct.DatasetBuilder : package
      Use `self.ds_consts.CONST_ACME` to access any required constant
    """
    return ct.DatasetBuilder

  @property
  def cv2(self):
    """
    provides access to computer vision library
    """
    return cv2

  @property
  def np(self):
    """
    Provides access to numerical processing library
    

    Returns
    -------
    np : Numpy package
      
    Example:
      ```
      np_zeros = self.np.zeros(shape=(10,10))
      ```
    """
    return np  
  
  @property
  def OrderedDict(self):
    """
    Returns the definition for `OrderedDict`

    Returns
    -------
    OrderedDict : class
      `OrderedDict` from standard python `collections` package.
      
    Example
    -------
        ```
        dct_A = self.OrderedDict({'a': 1})
        dct_A['b'] = 2
        ```

    """
    return OrderedDict  
  
  
  @property
  def defaultdict(self):
    """
    provides access to defaultdict class


    Returns
    -------
      defaultdict : class
      
    Example
    -------
      ```
        dct_integers = self.defaultdict(lambda: 0)
      ```

    """
    return defaultdict
  
  
  def DefaultDotDict(self, *args):
    """
    Returns a `DefaultDotDict` object that is a `dict` where you can use keys with dot 
    using the default initialization
    
    Inputs
    ------
    
      pass a `lambda: <type>` always
    
    Returns
    -------
      DefaultDotDict : class
     
    Example
    -------
     ```
       dct_dot = self.DefaultDotDict(lambda: str)
       dct_dot.test1 = "test"       
       print(dct_dot.test1)
       print(dct_dot.test2)
     ```

    """
    return DefaultDotDict(*args)
  
  def NestedDotDict(self, *args):
    """
    Returns a `NestedDotDict` object that is a `dict` where you can use keys with dot
    
    Returns
    -------
      defaultdict : class
     
    Example
    -------
     ```
       dct_dot = self.NestedDotDict({'test' : {'a' : 100}})
       dct_dot.test.a = "test"   
       print(dct_dot.test.a)
    """
    return NestedDotDict(*args)
  
  
  def NestedDefaultDotDict(self, *args):
    """
    Returns a `NestedDefaultDotDict` object that is a `defaultdict(dict)` where you can use keys with dot
    
    Returns
    -------
      defaultdict : class
     
    Example
    -------
     ```
      dct_dot1 = self.NestedDefaultDotDict()
      dct_dot1.test.a = "test"   
      print(dct_dot1.test.a)
       
      dct_dot2 = self.NestedDefaultDotDict({'test' : {'a' : 100, 'b' : {'c' : 200}}})
      print(dct_dot2.test.a)
      print(dct_dot2.test.b.c)
      print(dct_dot2.test.b.unk)
        
    """
    return NestedDefaultDotDict(*args)

  def LogReader(self, buff_reader, size=100):
    """
    Returns a `LogReader` object that is used to read from a buffer reader.

    Parameters
    ----------
    buff_reader : BufferedReader
        the buffer from where to read
    size : int, optional
        the size of the buffer. The default is 100.

    Returns
    -------
    LogReader : class
        the log reader object.
    """

    return LogReader(owner=self, buff_reader=buff_reader, size=size)

  def path_exists(self, path):
    """
    TODO: must be reviewed
    """
    return self.os_path.exists(path)
  
  
  @property
  def deque(self):
    """
    provides access to deque class
    """
    return deque  
  
  @property
  def datetime(self):
    """
    Proxy for the `datetime.datetime`

    Returns
    -------
      datetime : datetime object
      
      
    Example
    -------
      ```
      now = self.datetime.now()
      ```

    """
    return datetime

  @property
  def timezone(self):
    """
    Proxy for the `datetime.timezone`

    Returns
    -------
      timezone : timezone object
      
      
    Example
    -------
      ```
      utc = self.timezone.utc
      ```

    """
    return timezone

  @property
  def deepcopy(self):
    """
    This method allows us to use the method deepcopy
    """
    return deepcopy
  
  @property
  def os_path(self):
    """
    Proxy for `os.path` package


    Returns
    -------
      package
      
      
    Example
    -------
      ```
      fn = self.diskapi_save_dataframe_to_data(df, 'test.csv')
      exists = self.os_path.exists(fn)
      ```

    """
    return os.path
  
  @property
  def os_environ(self):
    """
    Returns a copy of the current environment variables based on `os.environ`.
    Important: Changing a value in the returned dictionary does NOT change 
               the value of the actual environment variable.
    

    Returns
    -------
    _type_
        _description_
    """
    return os.environ.copy()

  @property
  def PIL(self):
    """
    provides access to PIL package
    """
    return PIL

  @property
  def BytesIO(self):
    """
    provides access to BytesIO class from io package
    """
    return BytesIO

  @property
  def ElementTree(self):
    """
    provides access to ElementTree class from xml.etree package
    """
    return ElementTree

  @property
  def pd(self):
    """
    Provides access to pandas library

    Returns
    -------
      package
      
      
    Example
    -------
      ```
      df = self.pd.DataFrame({'a' : [1,2,3], 'b':[0,0,1]})      
      ```

    """
    return pd  

  @property
  def partial(self):
    """
    Provides access to `functools.partial` method

    Returns
    -------
      method


    Example
    -------
      ```
      fn = self.partial(self.diskapi_save_dataframe_to_data, fn='test.csv')
      ```

    """
    return partial

  def safe_json_dumps(self, dct, replace_nan=False, **kwargs):
    """Safe json dumps that can handle numpy arrays and so on

    Parameters
    ----------
    dct : dict
        The dict to be dumped
        
    replace_nan : bool, optional
        Replaces nan values with None. The default is False.

    Returns
    -------
    str
        The json string
    """
    return self.log.safe_json_dumps(dct, replace_nan=replace_nan, **kwargs)

  
  def json_dumps(self, dct, replace_nan=False, **kwargs):
    """Alias for `safe_json_dumps` for backward compatibility
    """
    return self.safe_json_dumps(dct, replace_nan=replace_nan, **kwargs)
  
  def json_loads(self, json_str, **kwargs):
    """
    Parses a json string and returns the dictionary
    """
    return self.json.loads(json_str, **kwargs)
  
  
  def load_config_file(self, fn):
    """
    Loads a json/yaml config file and returns the config dictionary

    Parameters
    ----------
    fn : str
      The filename of the config file

    Returns
    -------
    dict
      The config dictionary
    """
    return self.log.load_config_file(fn=fn)
  
  
  def maybe_download(self, url, fn, target='output', **kwargs):
    """
    Enables http/htps/minio download capabilities.


    Parameters
    ----------
    url : str or list
      The URI or URIs to be used for downloads
      
    fn: str of list
      The filename or filenames to be locally used
      
    target: str
      Can be `output`, `models` or `data`. Default is `output`

    kwargs: dict
      if url starts with 'minio:' the function will retrieve minio conn
             params from **kwargs and use minio_download (if needed or forced)

    Returns
    -------
      files, messages : list, list
        all the local files and result messages from download process
      
      
    Example
    -------
    """
    res = None
    files, msgs = self.log.maybe_download(
      url=url,
      fn=fn,
      target=target,
      **kwargs,
    )
    if len(files) >= 1:
      if len(files) == 1:
        res = files[0]
      else:
        res = files
    else:
      self.P('Errors while downloading: {}'.format([str(x) for x in msgs]))
    return res
  
  def git_clone(self, repo_url, repo_dir, target='output', user=None, token=None, pull_if_exists=True):
    """
    Clones a git repository or pulls if the repository already exists.

    Parameters
    ----------
    repo_url : str
      The git repository URL
      
    token : str, optional
      The token to be used for authentication. The default is None.
      
    user: str, optional
      The username to be used for authentication. The default is None.
          
    token : str, optional
      The token to be used for authentication. The default is None.
      
    pull_if_exists : bool, optional
      If True, the repository will be pulled if it already exists. The default is True.
      

    Returns
    -------
    str
      The local folder where the repository was cloned.
    """

    repo_path = self.os_path.join(self.get_target_folder(target), repo_dir)
    self.P(f"git_clone: '{repo_url}' to '{repo_path}'")

    if user is not None and token is not None:
      repo_url = repo_url.replace('https://', f'https://{user}:{token}@')
      
    USE_GIT_IGNORE_AUTH = True # for git pull -c does not work

    try:
      command = None
      if self.os_path.exists(repo_path) and pull_if_exists:
        # Repository already exists, perform git pull
        self.P(f"git_clone: Repo exists at {repo_path} -> pulling...")
        if USE_GIT_IGNORE_AUTH:
          command = ["git"] + GIT_IGNORE_AUTH + ["pull"]
        else:
          command = ["git", "pull"]
        results = subprocess.check_output(
            command,
            cwd=repo_path,
            stderr=subprocess.STDOUT,
            universal_newlines=True, 
            # creationflags=subprocess.CREATE_NO_WINDOW, # WARNING: This works only on Windows
        )
      else:
        # Clone the repository
        if USE_GIT_IGNORE_AUTH:
          command = ["git"] + GIT_IGNORE_AUTH + ["clone", repo_url, repo_path]
        else:
          command = ["git", "clone", repo_url, repo_path]
        results = subprocess.check_output(
            command,
            stderr=subprocess.STDOUT,
            universal_newlines=True, 
            # creationflags=subprocess.CREATE_NO_WINDOW, # WARNING: This works only on Windows
        )      
      # end if
      self.P(f"git_clone: `{' '.join(command)}` results:\n{results}")
    except subprocess.CalledProcessError as exc:
      self.P(f"git_clone: Error '{exc.cmd}' returned  {exc.returncode} with output:\n{exc.output}", color='r')
      repo_path = None
    except Exception as exc:
      self.P(f"git_clone: Error while cloning git repository {repo_url} in {repo_path}: {exc}", color='r  ')
      repo_path = None
    # end try
    return repo_path
  
    
  def git_get_local_commit_hash(self, repo_dir):
    """
    Retrieves the latest commit hash from the local git repository.

    Parameters
    ----------
    repo_dir : str
      The local directory where the repository is cloned.

    Returns
    -------
    str
      The latest commit hash from the local repository.
    """
    commit_hash = None
    def _P(msg, color=None):
      print(msg)
      return
    
    printer = self.P if hasattr(self, 'P') else _P
    printer(f"git_get_local_commit_hash: {repo_dir}")

    command = ["git", "rev-parse", "HEAD"]
    try:
      results = subprocess.check_output(
        command,
        cwd=repo_dir,
        stderr=subprocess.STDOUT,
        universal_newlines=True, 
        # creationflags=subprocess.CREATE_NO_WINDOW, # WARNING: This works only on Windows
      )
      if results is not None:
        printer(f"git_get_local_commit_hash: `rev-parse` results:\n{results}")
        lines = results.split('\n')
        if len(lines) > 0:
          commit_hash = lines[0].split()[0]        
      else:
        printer(f"git_get_local_commit_hash: Error while retrieving commit hash from remote repository: {results}", color='r')
    except subprocess.CalledProcessError as exc:
      printer(f"git_get_local_commit_hash: Error '{exc.cmd}' returned  {exc.returncode} with output:\n{exc.output}", color='r')
    except Exception as exc:
      printer(f"git_get_local_commit_hash: An unexpected exception occurred: {exc}", color='r')    
    return commit_hash
  
    
  def git_get_last_commit_hash(self, repo_url, user=None, token=None):
    """
    Retrieves the latest commit hash from the remote git repository.

    Parameters
    ----------
    repo_url : str
      The git repository URL

    user : str, optional
      The username to be used for authentication. The default is None.

    token : str, optional
      The token to be used for authentication. The default is None.

    Returns
    -------
    str
      The latest commit hash from the remote repository.
    """
    commit_hash = None
    def _P(msg, color=None):
      print(msg)
      return
    printer = self.P if hasattr(self, 'P') else _P
    printer(f"git_get_last_commit_hash: using {repo_url}")

    if user is not None and token is not None:
      repo_url = repo_url.replace('https://', f'https://{user}:{token}@')
      
    command = ["git"] + GIT_IGNORE_AUTH + ["ls-remote", repo_url, "HEAD"]
    try:
      results = subprocess.check_output(
        command,
        stderr=subprocess.STDOUT,
        universal_newlines=True, 
        # creationflags=subprocess.CREATE_NO_WINDOW, # WARNING: This works only on Windows
      )
      if results is not None:
        printer(f"git_get_last_commit_hash: `ls-remote` results:\n{results}")
        lines = results.split('\n')
        if len(lines) > 0:
          commit_hash = lines[0].split()[0]        
      else:
        printer(f"git_get_last_commit_hash: Error while retrieving commit hash from remote repository: {results}", color='r')
    except subprocess.CalledProcessError as exc:
      printer(f"git_get_last_commit_hash: Error '{exc.cmd}' returned  {exc.returncode} with output:\n{exc.output}", color='r')
    except Exception as exc:
      printer(f"git_get_last_commit_hash: An unexpected exception occurred: {exc}", color='r')    
    return commit_hash
  

  def indent_strings(self, strings, indent=2):
    """ Indents a string or a list of strings by a given number of spaces."""
    lst_strings = strings.split('\n')
    lst_strings = [f"{' ' * indent}{string}" for string in lst_strings]
    result = '\n'.join(lst_strings)
    return result
  


  def dict_to_str(self, dct:dict):
    """
    Transforms a dict into a pre-formatted strig without json package

    Parameters
    ----------
    dct : dict
      The given dict that will be string formatted.

    Returns
    -------
    str
      the nicely formatted.
      
      
    Example:
    -------
      ```
      dct = {
        'a' : {
          'a1' : [1,2,3]
        },
        'b' : 'abc'
      }
      
      str_nice_dict = self.dict_to_str(dct=dct)
      ```

    """
    return self.log.dict_pretty_format(dct)  
  
  def timestamp_to_str(self, ts=None, fmt='%Y-%m-%d %H:%M:%S'):
    """
    Returns the string representation of current time or of a given timestamp


    Parameters
    ----------
    ts : float, optional
      timestamp. The default is None and will generate string for current timestamp. 
    fmt : str, optional
      format. The default is '%Y-%m-%d %H:%M:%S'.


    Returns
    -------
    str
      the timestamp in string format.
      
    
    Example
    -------
        
      ```
      t1 = self.time()
      ...
      str_t1 = self.time_to_str(t1)
      result = {'T1' : str_t1}
      ```
    """
    if ts is None:
      ts = self.time()
    return self.log.time_to_str(t=ts, fmt=fmt)
  
  
  def time_to_str(self, ts=None, fmt='%Y-%m-%d %H:%M:%S'):
    """
    Alias for `timestamp_to_str`
    

    Parameters
    ----------
    ts : float, optional
      The given time. The default is None.
    fmt : str, optional
      The time format. The default is '%Y-%m-%d %H:%M:%S'.

    Returns
    -------
    str
      the string formatted time.
      
      
    Example
    -------
      ```
      t1 = self.time()
      ...
      str_t1 = self.time_to_str(t1)
      result = {'T1' : str_t1}
      ```

    """
    return self.timestamp_to_str(ts=ts, fmt=fmt)
  
  
  def datetime_to_str(self, dt=None, fmt='%Y-%m-%d %H:%M:%S'):
    """
    Returns the string representation of current datetime or of a given datetime

    Parameters
    ----------
    dt : datetime, optional
      a given datetime. The default is `None` and will generate string for current date.
    fmt : str, optional
      datetime format. The default is '%Y-%m-%d %H:%M:%S'.

    Returns
    -------
    str
      the datetime in string format.
      
    
    Example
    -------
      ```
      d1 = self.datetime()
      ...
      str_d1 = self.datetime_to_str(d1)
      result = {'D1' : str_d1}
      ```
    

    """
    if dt is None:
      dt = datetime.now()
    return datetime.strftime(dt, format=fmt)

  def time_in_interval_hours(self, ts, start, end):
    """
    Provides access to method `time_in_interval_hours` from .log
    Parameters
    ----------
    ts: datetime timestamp
    start = 'hh:mm'
    end = 'hh:mm'

    Returns
    -------

    """
    return self.log.time_in_interval_hours(ts, start, end)

  def time_in_schedule(self, ts, schedule, weekdays=None):
    """
    Check if a given timestamp `ts` is in a active schedule given the schedule data


    Parameters
    ----------
    ts : float
      the given timestamp.
      
    schedule : dict or list
      the schedule.
            
    weekdays : TYPE, optional
      list of weekdays. The default is None.


    Returns
    -------
    bool
      Returns true if time in schedule.
      

    Example
    -------
      ```
      simple_schedule = [["09:00", "12:00"], ["13:00", "17:00"]]
      is_working = self.time_in_schedule(self.time(), schedule=simple_schedule)
      ```

    """
    return self.log.time_in_schedule(
      ts=ts,
      schedule=schedule,
      weekdays=weekdays
    )
    
    
  


  def now_in_schedule(self, schedule, weekdays=None):
    """
    Check if the current time is in a active schedule given the schedule data


    Parameters
    ----------
    schedule : dict or list
      the schedule.
            
    weekdays : TYPE, optional
      list of weekdays. The default is None.


    Returns
    -------
    bool
      Returns true if time in schedule.
      

    Example
    -------
      ```
      simple_schedule = [["09:00", "12:00"], ["13:00", "17:00"]]
      is_working = self.now_in_schedule(schedule=simple_schedule)
      ```

    """
    return self.log.now_in_schedule(
      schedule=schedule,
      weekdays=weekdays
    )  
    
    
  def img_to_base64(self, img):
    """Transforms a numpy image into a base64 encoded image

    Parameters
    ----------
    img : np.ndarray
        the input image

    Returns
    -------
    str: base64 encoded image
    """
    return self.log.np_image_to_base64(img)

  def base64_to_img(self, b64):
    """
    Transforms a base64 encoded image into a np.ndarray
    Parameters
    ----------
    b64 : str
      the base64 image
    Returns
    -------
    np.ndarray: the decoded image
    """
    return self.log.base64_to_np_image(b64)

  
  def base64_to_str(self, b64, decompress=False, url_safe=False):
    """Transforms a base64 encoded string into a normal string

    Parameters
    ----------
    b64 : str
        the base64 encoded string
        
    decompress : bool, optional
        if True, the string will be decompressed after decoding. The default is False.

    Returns
    -------
    str: the decoded string
    """
    b_encoded = b64.encode('utf-8')
    if url_safe:
      b_text = base64.urlsafe_b64decode(b_encoded)
    else:
      b_text = base64.b64decode(b_encoded)
      
    if decompress:
      b_text = zlib.decompress(b_text)
    str_text = b_text.decode('utf-8')
    return str_text

  def execute_remote_code(self, code: str, debug: bool = False, timeout: int = 10):
    """
    Execute code received remotely.
    Parameters
    ----------
    code : str
        the code to be executed
    debug : bool, optional
        if True, the code will be executed in debug mode. The default is False.
    timeout : int, optional
        the timeout for the code execution. The default is 10.
    Returns
    -------
    dict: the result of the code execution
    If the code execution was successful, the result will contain the following keys:
    - result: the result of the code execution
    - errors: the errors that occurred during the execution
    - warnings: the warnings that occurred during the execution
    - prints: the printed messages during the execution
    - timestamp: the timestamp of the execution
    If the code execution failed, the result will contain the following key:
    - error: the error message
    """
    if not isinstance(code, str):
      return {'error': 'Code must be a string'}
    if len(code) == 0:
      return {'error': 'Code should not be an empty string'}
    result, errors, warnings, printed = None, None, [], []
    self.P(f'Executing code:\n{code}')
    b64_code, errors = self.code_to_base64(code, return_errors=True)
    if errors is not None:
      return {'error': errors}
    res = self.exec_code(
      str_b64code=b64_code,
      debug=debug,
      self_var='plugin',
      modify=True,
      return_printed=True,
      timeout=timeout
    )
    if isinstance(res, tuple):
      result, errors, warnings, printed = res
    return {
      'result': result,
      'errors': errors,
      'warnings': warnings,
      'prints': printed,
      'timestamp': self.time()
    }

  def image_entropy(self, image):
    """
    Computes the entropy of an image.

    Parameters
    ----------
    image : cv2 image | PIL image | np.ndarray
        the input image.

    Returns
    -------
    entropy: float
        the entropy of the image
    """

    if image is None:
      # self.P("Image is None")
      return 0

    np_image = np.array(image)
    entropy = 0

    np_marg = np.histogramdd(np.ravel(np_image), bins=256)[0] / np_image.size
    np_marg = np_marg[np.where(np_marg > 0)]
    entropy = -np.sum(np.multiply(np_marg, np.log2(np_marg)))

    return entropy

  def shorten_str(self, s, max_len=32):
    """
    Shortens a string to a given max length.
    Parameters
    ----------
    s : str | list | dict
    max_len : int, optional

    Returns
    -------
    str | list | dict : the shortened string
    """
    if isinstance(s, str):
      return s[:max_len] + '...' if len(s) > max_len else s
    if isinstance(s, list):
      return [self.shorten_str(x, max_len) for x in s]
    if isinstance(s, dict):
      return {k: self.shorten_str(v, max_len) for k, v in s.items()}
    return s

  def normalize_text(self, text):
    """
    Uses unidecode to normalize text. Requires unidecode package

    Parameters
    ----------
    text : str
      the proposed text with diacritics and so on.

    Returns
    -------
    text : str
      decoded text if unidecode was avail



    Example
    -------
      ```
      str_txt = "Ha ha ha, m\u0103 bucur c\u0103 ai \u00eentrebat!"
      str_simple = self.normalize_text(str_text)
      ```


    """
    text = text.replace('\t', '  ')
    try:
      from unidecode import unidecode
      text = unidecode(text)
    except:
      pass
    return text  
  
  
  def sanitize_name(self, name: str)->str:
    """
    Returns a sanitized name that can be used as a variable name

    Parameters
    ----------
    name : str
        the proposed name

    Returns
    -------
    str
        the sanitized name
    """
    return re.sub(r'[^\w\.-]', '_', name)
  
  def convert_size(self, size, unit):
    """
    Given a size and a unit, it returns the size in the given unit

    Parameters
    ----------
    size : int
        value to be converted
    unit : str
        one of the following: 'KB', 'MB', 'GB'

    Returns
    -------
    _type_
        _description_
    """
    new_size = size
    if unit == ct.FILE_SIZE_UNIT.KB:
      new_size = size / 1024
    elif unit == ct.FILE_SIZE_UNIT.MB:
      new_size = size / 1024**2
    elif unit == ct.FILE_SIZE_UNIT.GB:
      new_size = size / 1024**3
    return new_size  
  
  def managed_lock_resource(self, str_res, condition=True):
    """
    Managed lock resource. Will lock and unlock resource automatically. 
    To be used in a with statement.
    The condition parameter allows users to disable the lock if desired.

    Parameters
    ----------
    str_res : str
      The resource to lock.
    condition : bool, optional
      If False the lock will not be acquired. The default is True.

    Returns
    -------
    LockResource
      The lock resource object.

    Example
    -------
    ```
    with self.managed_lock_resource('my_resource'):
      # do something
    ```

    ```
    # will control if the following operation is locked or not based on this flag
    locking = False
    with self.managed_lock_resource('my_resource', condition=locking):
      # do something
    ```
    """
    return self.log.managed_lock_resource(str_res=str_res, condition=condition)

  def lock_resource(self, str_res):
    """
    Locks a resource given a string. Alias to `self.log.lock_resource`

    Parameters
    ----------
    str_res : str
        the resource name
    """
    return self.log.lock_resource(str_res)

  def unlock_resource(self, str_res):
    """
    Unlocks a resource given a string. Alias to `self.log.unlock_resource`

    Parameters
    ----------
    str_res : str
        the resource name
    """
    return self.log.unlock_resource(str_res)

  def create_numpy_shared_memory_object(self, mem_name, mem_size, np_shape, np_type, create=False, is_buffer=False, **kwargs):
    """
    Create a shared memory for numpy arrays. 
    This method returns a `NumpySharedMemory` object that can be used to read/write numpy arrays from/to shared memory.
    Use this method instead of creating the object directly, as it requires the logger to be set.

    For a complete set of parameters, check the `NumpySharedMemory` class from `core.utils.system_shared_memory`

    Parameters
    ----------
    mem_name : str
        the name of the shared memory
    mem_size : int
        the size of the shared memory. can be ignored if np_shape is provided
    np_shape : tuple
        the shape of the numpy array. can be ignored if mem_size is provided
    np_type : numpy.dtype
        the type of the numpy array
    create : bool, optional
        create the shared memory if it does not exist, by default False
    is_buffer : bool, optional
        if True, the shared memory will be used as a buffer, by default False


    Returns
    -------
    NumPySharedMemory
        the shared memory object
    """
    
    return NumpySharedMemory(
      mem_name=mem_name,
      mem_size=mem_size,
      np_shape=np_shape,
      np_type=np_type,
      create=create,
      is_buffer=is_buffer,
      log=self.log,
      **kwargs
    )
    
    
  def get_temperature_sensors(self, as_dict=True):
    """
    Returns the temperature of the machine if available

    Returns
    -------
    dict
      The dictionary contains the following:
      - 'message': string indicating the status of the temperature sensors
      - 'temperatures': dict containing the temperature sensors
    """
    return self.log.get_temperatures(as_dict=as_dict)

  @property
  def bs4(self):
    """
    Provides access to the bs4 library

    Returns
    -------
      package


    Example
    -------
      ```

      response = self.requests.get(url)
      soup = self.bs4.BeautifulSoup(response.text, "html.parser")
      ```

    """
    return bs4
  
  def get_gpu_info(self, device_id=0):
    """
    Returns the GPU information
    
    Parameters
    ----------
    device_id : int, optional
      The device id. The default is 0.
      
    Returns
    -------
    dict
      The dictionary containing the GPU information
    """
    return self.log.get_gpu_info(device_id=device_id)


  
  

  def string_to_base64(self, txt, compress=False, url_safe=False):
    """Transforms a string into a base64 encoded string

    Parameters
    ----------
    txt : str
        the input string
        
    compress : bool, optional
        if True, the string will be compressed before encoding. The default is False.

    Returns
    -------
    str: base64 encoded string
    """
    b_text = bytes(txt, 'utf-8')    
    if compress:
      b_code = zlib.compress(b_text, level=9)
    else:
      b_code = b_text
    if url_safe:
      b_encoded = base64.urlsafe_b64encode(b_code)
    else:
      b_encoded = base64.b64encode(b_code)
    str_encoded = b_encoded.decode('utf-8')
    return str_encoded
  
  
  def str_to_base64(self, txt, compress=False, url_safe=False):
    """
    Alias for `string_to_base64`
    """
    return self.string_to_base64(txt, compress=compress, url_safe=url_safe)
  
  
  def dict_in_dict(self, dct1 : dict, dct2 : dict):
    """
    Check if dct1 is in dct2

    Parameters
    ----------
    dct1 : dict
        the first dictionary
    dct2 : dict
        the dictionary where we check if dct1 is contained in 

    Returns
    -------
    bool
        True if dct1 is in dct2
    """
    return self.log.match_template(dct2, dct1)


  def receive_and_decrypt_payload(self, data, verbose=0):
    """
    Method for receiving and decrypting a payload addressed to us.
    
    Parameters
    ----------
    data : dict
        The payload to be decrypted.
        
    verbose : int, optional
        The verbosity level. The default is 0.
        
        
    Returns
    -------
    dict
        The decrypted payload addressed to us.
    """
    # Extract the sender, the data and if the data is encrypted.
    sender = data.get(self.const.PAYLOAD_DATA.EE_SENDER, None)
    is_encrypted = data.get(self.const.PAYLOAD_DATA.EE_IS_ENCRYPTED, False)
    encrypted_data = data.get(self.const.PAYLOAD_DATA.EE_ENCRYPTED_DATA, None)
    # Remove the encrypted data from the payload data if it exists.
    result = {k: v for k, v in data.items() if k != self.const.PAYLOAD_DATA.EE_ENCRYPTED_DATA}

    if is_encrypted and encrypted_data:
      # Extract the destination and check if the data is addressed to us.
      dest = data.get(self.const.PAYLOAD_DATA.EE_DESTINATION, [])
      if not isinstance(dest, list):
        dest = [dest]
      # now we check if the data is addressed to us
      if self.e2_addr not in dest:
        # TODO: maybe still return the encrypted data for logging purposes
        if verbose > 0:
          self.P(f"Payload data not addressed to us. Destination: {dest}. Ignoring.")
        # endif verbose
        return {}
      # endif destination check

      try:
        # This should fail in case the data was not sent to us.
        str_decrypted_data = self.bc.decrypt_str(
          str_b64data=encrypted_data, str_sender=sender,
          # embed_compressed=True, # we expect the data to be compressed
        )
        decrypted_data = self.json_loads(str_decrypted_data)
      except Exception as exc:
        self.P(f"Error while decrypting payload data from {sender}:\n{exc}")
        if verbose > 0:
          self.P(f"Received data:\n{self.dict_to_str(result)}")
        # endif verbose
        decrypted_data = None
      # endtry decryption
      
      if decrypted_data is not None:
        # If the decrypted data is not a dictionary, we embed it in a dictionary.
        # TODO: maybe review this part
        if not isinstance(decrypted_data, dict):
          decrypted_data = {'EE_DECRYPTED_DATA': decrypted_data}
        # endif not dict
        if verbose > 0:
          decrypted_keys = list(decrypted_data.keys())
          self.P(f"Decrypted data keys: {decrypted_keys}")
        # endif verbose
        # Merge the decrypted data with the original data.
        result = {
          **result,
          **decrypted_data
        }
      else:
        if verbose > 0:
          self.P(f"Decryption failed. Returning original data.")
        # endif verbose
      # endif decrypted_data is not None
    # endif is_encrypted
    return result


  def check_payload_data(self, data, verbose=0):
    """
    Method for checking if a payload is addressed to us and decrypting it if necessary.
    Parameters
    ----------
    data : dict
        The payload data to be checked and maybe decrypted.

    verbose : int, optional
        The verbosity level. The default is 0.
    Returns
    -------
    dict
        The original payload data if not encrypted.
        The decrypted payload data if encrypted and the payload was addressed to us.
        None if the payload was encrypted but not addressed to us.
    """
    return self.receive_and_decrypt_payload(data=data, verbose=verbose)
  
  
  def get_hash(self, str_data: str, length=None, algorithm='md5'):
    """
    This method returns the hash of a given string.
    
    Parameters
    ----------
    str_data : str
        The string to be hashed.
    
    length : int, optional
        The length of the hash. The default is None.
        
    algorithm : str, optional
        The algorithm to be used. The default is 'md5'.
        
    Returns
    -------
    
    str
        The hash of the string.
        
    Example
    -------
    
    ```
    hash = plugin.get_hash('test', length=8, algorithm='md5')
    ```
    
    
    """
    assert algorithm in ['md5', 'sha256'], f"Invalid algorithm: {algorithm}"
    bdata = bytes(str_data, 'utf-8')
    if algorithm == 'md5':
      h = hashlib.md5(bdata)
    elif algorithm == 'sha256':
      h = hashlib.sha256(bdata)
    result = h.hexdigest()[:length] if length is not None else h.hexdigest()
    return result


# endclass _UtilsBaseMixin



if __name__ == '__main__':
  from naeural_core import Logger
  from copy import deepcopy

  log = Logger("UTL", base_folder='.', app_folder='_local_cache')

  e = _UtilsBaseMixin()
  e.log = log  
  e.P = print
  
  TEST_D_IN_D = True
  TEST_DICTS = False
  TEST_GIT = False
  
  if TEST_D_IN_D:
    d2 = {
      "SIGNATURE" : "TEST1",
      "DATA" : {
        "A" : 1,
        "B" : 2,
        "C" : {
          "C1" : 10,
          "C2" : 20
        }
      }
    }
    
    d10 = {
      "SIGNATURE" : "TEST1",
      "DATA" : {
        "C" : {
          "C2" : 20
        }
      }
    }
    
    d11 = {
      "SIGNATURE" : "TEST1",
      "DATA" : {
        "C" : {
          "C2" : 1,
        }
      }
    }
    
    log.P("Test 1: d10 in d2: {}".format(e.dict_in_dict(d10, d2)))
    log.P("Test 2: d11 in d2: {}".format(e.dict_in_dict(d11, d2)))
  
  if TEST_DICTS:

    d1 = e.DefaultDotDict(str)
    d1.a = "test"
    print(d1.a)
    print(d1.c)

    d1 = e.DefaultDotDict(lambda: str, {'a' : 'test', 'b':'testb'})
    print(d1.a)
    print(d1.b)
    print(d1.c)
    
    d1c = deepcopy(d1)
    
    d20 = {'k0':1, 'k1': {'k11': 10, 'k12': [{'k111': 100, 'k112':200}]}}
    d2 = e.NestedDotDict(d20)
    d20c = deepcopy(d20)
    d2c = deepcopy(d2)
    
    print(d2)
    print(d2.k0)
    print(d2.k1.k12[0].k112)
    
    
    
    d3 = defaultdict(lambda: DefaultDotDict({'timestamp' : None, 'data' : None}))
    
    s = json.dumps(d20)
    print(s)
    
    b64 = e.string_to_base64(s)
    print("{}: {}".format(len(b64), b64[:50]))
    print(e.base64_to_str(b64))

    b64c = e.string_to_base64(s, compress=True)
    print("{}: {}".format(len(b64c), b64c[:50]))
    print(e.base64_to_str(b64c, decompress=True))
      
    config = e.load_config_file(fn='./config_startup.txt')
    

    d4 = NestedDefaultDotDict()
    
    assert d4.test == {}, "Accessing an undefined key did not return empty dict."
    
    # Test case 2: Automatically creates nested dictionaries and sets value
    d4.test2.x = 5
    assert d4.test2.x == 5, "Nested assignment failed."
    
    # Test case 3: Auto-creates both test3 and test4, where test4 has value None
    _ = d4.test3.test4  # Access to create
    assert len(d4.test3) != 0 and len(d4.test3.test4) == 0, "Nested auto-creation failed."
    
    print("All tests passed.")
  
  if TEST_GIT:
    repo = 'https://github.com/Ratio1/edge_node_launcher'
    sub_folder = "test_repo"
    full_path = os.path.join(log.get_output_folder(), sub_folder)
    
    
    # cloning
    local_path = e.git_clone(repo_url=repo, repo_dir=sub_folder, pull_if_exists=True)
    # remote hash
    remote_hash = e.git_get_last_commit_hash(repo_url=repo)
    # local hash
    local_hash = e.git_get_local_commit_hash(repo_dir=local_path)
    # output
    log.P(f"Cloned to: <{local_path}>")
    log.P(f"Remote: <{remote_hash}>")
    log.P(f"Local:  <{local_hash}>")
    