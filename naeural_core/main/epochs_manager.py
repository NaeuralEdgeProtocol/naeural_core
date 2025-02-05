"""
TODO:
 This data MUST be delivered via dAuth to all nodes in the network.

# Mainnet & Testnet

EE_GENESIS_EPOCH_DATE="2025-02-03 17:00:00"
EE_EPOCH_INTERVALS=24
EE_EPOCH_INTERVAL_SECONDS=3600

# Devnet
EE_GENESIS_EPOCH_DATE="2025-01-24 00:00:00"
EE_EPOCH_INTERVALS=1
EE_EPOCH_INTERVAL_SECONDS=3600


  
"""
import uuid
import json
import os

import numpy as np

from datetime import datetime, timedelta, timezone
from collections import defaultdict, deque
from copy import deepcopy
from threading import Lock
from time import time


from naeural_core import constants as ct
from naeural_core.utils import Singleton


EPOCH_MANAGER_VERSION = '0.2.2'

#############################################
#############################################
DEFAULT_GENESYS_EPOCH_DATE = "2025-01-24 00:00:00"      # "2025-02-03 17:00:00"  # OLD: "2024-03-10 00:00:00"
DEFAULT_EPOCH_INTERVALS = 1                     # 24
DEFAULT_EPOCH_INTERVAL_SECONDS = 3600           # 3600
SUPERVISOR_MIN_AVAIL_PRC = 0.60                 # 0.98 for mainnet, 60% for testnet
#############################################
#############################################

DEFAULT_NODE_ALERT_INTERVAL = DEFAULT_EPOCH_INTERVAL_SECONDS


EPOCH_MAX_VALUE = 255

SUPERVISOR_MIN_AVAIL_UINT8 = int(SUPERVISOR_MIN_AVAIL_PRC * EPOCH_MAX_VALUE)

FN_NAME = 'epochs_status.pkl'
FN_SUBFOLDER = 'network_monitor'
FN_FULL = FN_SUBFOLDER + '/' + FN_NAME

EPOCHMON_MUTEX = 'epochmon_mutex'


INITIAL_SYNC_EPOCH = 0 # TODO: add initial sync epoch

try:
  EPOCH_MANAGER_DEBUG = int(os.environ.get(ct.EE_EPOCH_MANAGER_DEBUG, 1))
except Exception as e:
  EPOCH_MANAGER_DEBUG = 1

SYNC_SIGNATURES = 'SIGNATURES'
SYNC_VALUE = 'VALUE'
SYNC_LAST_EPOCH = 'LAST_SYNC_EPOCH'
SYNC_NODES = 'NODES'

SYNC_SAVES_TS = 'SAVES_UTC'
SYNC_SAVES_EP = 'SAVE_EPOCHS'
SYNC_RESTARTS = 'EM_RESTARTS_UTC'
SYNC_RELOADS = 'EM_RELOADS_UTC'

_FULL_DATA_TEMPLATE_EXTRA = {
  SYNC_SAVES_TS : [],
  SYNC_SAVES_EP : [],
  SYNC_RESTARTS : [],
  SYNC_RELOADS : [],
  
  ct.EE_GENESIS_EPOCH_DATE_KEY : None,
  ct.BASE_CT.EE_EPOCH_INTERVALS_KEY : None,
  ct.BASE_CT.EE_EPOCH_INTERVAL_SECONDS_KEY : None,
}

_FULL_DATA_MANDATORY_FIELDS = [
  SYNC_NODES,
  ct.EE_GENESIS_EPOCH_DATE_KEY ,
  ct.BASE_CT.EE_EPOCH_INTERVALS_KEY,
  ct.BASE_CT.EE_EPOCH_INTERVAL_SECONDS_KEY ,
]

_FULL_DATA_INFO_KEYS = [
  SYNC_SAVES_TS,
  SYNC_SAVES_EP,
  SYNC_RESTARTS,
  SYNC_RELOADS,
]

SYNC_HISTORY_SIZE = 10

class EPCT:
  NAME = 'name'
  ID = 'id'
  EPOCHS = 'epochs'
  ALERTS = 'alerts'
  LAST_ALERT_TS = 'last_alert_ts'
  CURRENT_EPOCH = 'current_epoch'
  HB_TIMESTAMPS = 'hb_dates'
  HB_COUNT = 'hb_count'
  FIRST_SEEN = 'first_seen'
  LAST_SEEN = 'last_seen'
  LAST_EPOCH = 'last_epoch'
  
  LAST_EPOCH_RESTARTS = 'last_epoch_restarts'

  SIGNATURES = 'signatures'
  

_NODE_TEMPLATE = {
  EPCT.NAME           : None,
  EPCT.EPOCHS         : defaultdict(int),
  EPCT.ALERTS         : 0,
  EPCT.LAST_ALERT_TS  : 0,
  EPCT.FIRST_SEEN     : None,    
  EPCT.LAST_SEEN      : None,  

  EPCT.SIGNATURES     : defaultdict(list),
  
  EPCT.LAST_EPOCH_RESTARTS : [], # this will not function without a save-reload mechanism
  
  
  EPCT.CURRENT_EPOCH  : {
    EPCT.ID               : None,
    EPCT.HB_TIMESTAMPS   : set(),
  },
  
  EPCT.LAST_EPOCH : {
    EPCT.ID : None,
    EPCT.HB_TIMESTAMPS : set(),
  }
}

def str2date(date_str):
  return datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")

def _get_node_template(name):
  data = deepcopy(_NODE_TEMPLATE)
  data[EPCT.NAME] = name
  return data


class EpochsManager(Singleton):
  
  def build(self, owner, debug_date=None, debug=None):
    """

    """
    if debug is None:
      debug = EPOCH_MANAGER_DEBUG
    self.__epoch_intervals = DEFAULT_EPOCH_INTERVALS
    self.__epoch_interval_seconds = DEFAULT_EPOCH_INTERVAL_SECONDS
    self._epoch_interval_setup()
    
    self.__last_state_log = 0

    # for Genesis epoch date is correct to replace in order to have a timezone aware date
    # and not consider the local timezone
    genesis_epoch_date_env = str(os.environ.get(ct.EE_GENESIS_EPOCH_DATE_KEY, DEFAULT_GENESYS_EPOCH_DATE))
    if len(genesis_epoch_date_env) != len(DEFAULT_GENESYS_EPOCH_DATE):
      genesis_epoch_date_env = DEFAULT_GENESYS_EPOCH_DATE
    self.__genesis_date_str = genesis_epoch_date_env
    self.__genesis_date = self.log.str_to_date(self.__genesis_date_str).replace(tzinfo=timezone.utc)
    
    _FULL_DATA_TEMPLATE_EXTRA[ct.EE_GENESIS_EPOCH_DATE_KEY] = self.__genesis_date_str
    _FULL_DATA_TEMPLATE_EXTRA[ct.BASE_CT.EE_EPOCH_INTERVALS_KEY] = self.__epoch_intervals
    _FULL_DATA_TEMPLATE_EXTRA[ct.BASE_CT.EE_EPOCH_INTERVAL_SECONDS_KEY] = self.__epoch_interval_seconds

    self.owner = owner
    self.__current_epoch = None
    self.__data = {}
    self.__full_data = {}
    self.__eth_to_node = {}
    try:
      debug = int(debug)
    except Exception as e:
      self.P("Error setting debug: {}".format(e), color='r')
      debug = 1
    self.__debug = debug
    self._set_dbg_date(debug_date)

    loaded = self._load_status()

    self.P(
      "EpochsMgr v{}, dbg:{}, epoch #{}, GENESIS=[{}] Int/Ep: {}, Sec/Int: {} ".format(
        EPOCH_MANAGER_VERSION, self.__debug, 
        self.get_current_epoch(), self.__genesis_date_str,
        self.__epoch_intervals, self.__epoch_interval_seconds,
      ),
      color='m',
      boxed=True
    )
    return

  @property
  def data(self):
    return self.__data
  
  @property
  def full_data(self):
    return self.__full_data


  @property
  def genesis_date(self):
    return self.__genesis_date
  
  @property
  def epoch_length(self):
    return self.__epoch_intervals * self.__epoch_interval_seconds
  
  
  

  def _epoch_interval_setup(self):    
    try:
      self.__epoch_intervals = int(os.environ.get(
        ct.BASE_CT.EE_EPOCH_INTERVALS_KEY, DEFAULT_EPOCH_INTERVALS
      ))
    except Exception as e:
      self.P("Error setting epoch intervals: {}".format(e), color='r')
      
    try:
      self.__epoch_interval_seconds = int(os.environ.get(
        ct.BASE_CT.EE_EPOCH_INTERVAL_SECONDS_KEY, DEFAULT_EPOCH_INTERVAL_SECONDS
      ))
    except Exception as e:
      self.P("Error setting epoch interval seconds: {}".format(e), color='r')
    
    self.__node_alert_interval = self.__epoch_interval_seconds    
    return


  def _set_dbg_date(self, debug_date):
    """
    Is a str is given the date is assumed to be UTC based.
    """
    if debug_date is not None:
      if isinstance(debug_date, str):
        # this is correct and you are supposed to use a UTC based date string
        debug_date = self.log.str_to_date(debug_date).replace(tzinfo=timezone.utc)
    self._debug_date = debug_date
    return


  def P(self, msg, **kwargs):
    self.log.P('[EPM] ' + msg, **kwargs)
    return


  def start_timer(self, name):
    self.log.start_timer(name, section='epoch')
    return
  
  def stop_timer(self, name):
    self.log.stop_timer(name, section='epoch')
    return
  
  def __compute_eth_to_internal(self):
    if not hasattr(self.owner, "node_address_to_eth_address"):
      return
    for node_addr in self.__data:
      eth_node_addr = self.owner.node_address_to_eth_address(node_addr)
      self.__eth_to_node[eth_node_addr] = node_addr
    return
  
  def eth_to_internal(self, eth_node_addr):
    return self.__eth_to_node.get(eth_node_addr, None)
  
  def get_node_name(self, node_addr):
    """ 
    Given a node address, returns the name of the node.
    """
    return self.owner.network_node_eeid(node_addr)
  
  def __get_max_hb_per_epoch(self):
    max_hb = 0
    addr = self.owner.node_addr
    eeid = self.owner.node_name
    interval = self.owner.network_node_hb_interval(addr=addr)
    if interval is None:
      raise ValueError("Heartbeat interval not found for node: {} ({})".format(addr, eeid))
    nr_hb = 24 * 3600 // interval
    return nr_hb
  
  
  def __debug_status(self):
    if self.__debug:
      self.get_stats(display=True)
    #endif debug
    return
  
  
  def __trim_history(self, trimmable_type=(list, tuple, deque)):
    for full_data_key in _FULL_DATA_TEMPLATE_EXTRA:
      data = self.__full_data[full_data_key]
      is_trimable = isinstance(data, trimmable_type)
      if is_trimable:
        data_len = len(data)
        if data_len > SYNC_HISTORY_SIZE:
          self.__full_data[full_data_key] = self.__full_data[full_data_key][-SYNC_HISTORY_SIZE:]
      else:
        if full_data_key not in self.__full_data:
          self.__full_data[full_data_key] = _FULL_DATA_TEMPLATE_EXTRA[full_data_key] 
    return


  def save_status(self):
    with self.log.managed_lock_resource(EPOCHMON_MUTEX):
      self.__save_status()
    return

      
  def __save_status(self):
    """
    Saves the epochs status to disk called ONLY by `maybe_close_epoch` method - uses the critical section of the maybe_close_epoch method.
    If used separately, make sure to use a lock.
    """
    self.P(f"{self.__class__.__name__} saving epochs status for {len(self.__data)} nodes...")
    
    self.__full_data[SYNC_SAVES_TS].append(self.date_to_str())
    self.__full_data[SYNC_SAVES_EP].append(self.__current_epoch)
    self.__trim_history()
    _full_data_copy = deepcopy(self.__full_data) # maybe not needed

    self.log.save_pickle_to_data(
      data=_full_data_copy, 
      fn=FN_NAME,
      subfolder_path=FN_SUBFOLDER,
    )
    return
  
  
  def _load_status(self):
    """
    NOTE: 2025-01-23 / AID: 
    ----------------------------
    This method is called only once at the beginning of the class initialization and it will
    load the data previously saved to disk (via `__save_status` method) that in turn is called
    by `maybe_close_epoch` method. Thus the status is saved only when the epoch changes (and the
    hb timestamps are reset).
    This means that if a restart is done during a epoch, the data will be loaded from the last 
    reset resulting in a loss of data for the current epoch and the invalidation of the node
    capacity to act as a validator for the current epoch. This is a security feature to prevent
    fraud.
    ------------------------------------------------
    HOWEVER for TRUSTED nodes a procedure of save-reload should be implemented to ensure the
    data is not lost in case of a restart during an epoch but rather preserved and reloaded.
        
    """
    result = False
    exists = self.log.get_data_file(FN_FULL) is not None
    if exists:
      self.P("Previous epochs state found. Current oracle era specs:\n{}".format(
        json.dumps(self.get_era_specs(), indent=2)
      ))
      _full_data = self.log.load_pickle_from_data(
        fn=FN_NAME,
        subfolder_path=FN_SUBFOLDER
      )
      if _full_data is not None:
        missing_fields = False
        try:
          dct_to_display = {k:v for k,v in _full_data.items() if k != SYNC_NODES}
          self.P("Loaded epochs status with {} (current={}) nodes and specs:\n{}".format(
            len(_full_data.get(SYNC_NODES, [])),len(self.__data), json.dumps(dct_to_display, indent=2)
          ))
          for field in _FULL_DATA_MANDATORY_FIELDS:
            if field not in _full_data:
              missing_fields = True
              self.P(f"Missing mandatory field: {field}", color='r')
            # endif field not present
          # endfor mandatory fields
        except Exception as e:
          self.P(f"Error loading epochs status: {e}\n", color='r')
          missing_fields = True
        # end try-except
        if missing_fields:
          # old format
          self.P("Attempting to load old epochs status format. Dropping data", color='r')
          self.__full_data = {
            SYNC_NODES : self.__data,
            SYNC_LAST_EPOCH : INITIAL_SYNC_EPOCH,
          }
        else:
          # new format
          loaded_genesis_date = _full_data.get(ct.EE_GENESIS_EPOCH_DATE_KEY, self.__genesis_date_str)
          loaded_intervals = _full_data.get(ct.BASE_CT.EE_EPOCH_INTERVALS_KEY, self.__epoch_intervals)
          loaded_interval_seconds = _full_data.get(ct.BASE_CT.EE_EPOCH_INTERVAL_SECONDS_KEY, self.__epoch_interval_seconds)
          if (
            loaded_genesis_date != self.__genesis_date_str or
            loaded_intervals != self.__epoch_intervals or
            loaded_interval_seconds != self.__epoch_interval_seconds
          ):
            self.P(
              f"Wrong epoch conf: {loaded_genesis_date}, {loaded_intervals}, {loaded_interval_seconds} vs {self.__genesis_date_str}, {self.__epoch_intervals}, {self.__epoch_interval_seconds}", 
              color='r', boxed=True,
            )
          else:
            # loaded data is full data 
            self.__full_data = _full_data
            self.__data = _full_data[SYNC_NODES]
        # end if using new format
        result = True
      else:
        self.P("Error loading epochs status.", color='r')
    else:
      self.P("No previous epochs status found in {FN_FULL}.", color='r')

    self.__add_empty_fields()
    self.__compute_eth_to_internal()
    if result:
      self.__full_data[SYNC_RELOADS].append(self.date_to_str())
      self.P(f"Epochs status loaded with {len(self.__data)} nodes", boxed=True)
    #endif exists
    self.__debug_status()
    return result

  def __add_empty_fields(self):
    """
    Use this method to add missing fields to the loaded data structure.

    """
      
    template = deepcopy(_NODE_TEMPLATE)
    for node_addr in self.__data:
      for key in template:
        if key not in self.__data[node_addr]:
          self.__data[node_addr][key] = template[key]

    if SYNC_NODES not in self.__full_data:
      self.__full_data[SYNC_NODES] = self.__data
          
    template2 = deepcopy(_FULL_DATA_TEMPLATE_EXTRA) # here we load the epoch specs
    for full_date_key in template2:
      if full_date_key not in self.__full_data:
        self.__full_data[full_date_key] = template2[full_date_key]
    return

  def get_epoch_id(self, date : any):
    """
    Given a date as string or datetime, returns the epoch id - ie the number of days since 
    the genesis epoch.

    Parameters
    ----------
    date : str or date
      The date as string that will be converted to epoch id.
    """
    if isinstance(date, str):
      # remove milliseconds from string
      date = date.split('.')[0]
      date = self.log.str_to_date(date)
      # again this is correct to replace in order to have a timezone aware date
      # and not consider the local timezone. the `date` string naive should be UTC offsetted
      date = date.replace(tzinfo=timezone.utc) 
    # compute difference between date and self.__genesis_date in seconds
    elapsed_seconds = (date - self.__genesis_date).total_seconds()
    
    # the epoch id starts from 0 - the genesis epoch
    # the epoch id is the number of days since the genesis epoch
    # # TODO: change this if we move to start-from-one offset by adding +1
    # OBS: epoch always ends at AB:CD:59 no matter what 
    epoch_id = int(elapsed_seconds / self.epoch_length) 
    return epoch_id
  
  def epoch_to_date(self, epoch_id=None):
    """
    Given an epoch id, returns the date as string.

    Parameters
    ----------
    epoch_id : int
      the epoch id
    """
    if epoch_id is None:
      epoch_id = self.get_time_epoch()
    # TODO: change this if we move to start-from-one offset with (epoch_id - 1)
    date = self.__genesis_date + timedelta(seconds=(epoch_id * self.epoch_length))
    str_date = datetime.strftime(date, format="%Y-%m-%d %H:%M:%S")
    return str_date
  
  def date_to_str(self, date : datetime = None, move_to_utc : bool = False):
    """
    Converts a date to string.
    """
    if date is None:
      date = self.get_current_date()
    if move_to_utc:
      # as you pass a date with timezone info, the conversion to UTC is done by astimezone
      # and then the date is converted to string
      date = date.astimezone(timezone.utc)
    return datetime.strftime(date, format=ct.HB.TIMESTAMP_FORMAT_SHORT)
  
    
  
  def get_current_date(self):
    if self._debug_date is not None:
      return self._debug_date
    else:
      # we convert local time to UTC time
      return datetime.now(timezone.utc)
        
  def get_time_epoch(self):
    """
    Returns the current epoch id.
    """
    return self.get_epoch_id(self.get_current_date())
  
  def get_current_epoch(self):
    """
    Returns the current epoch id using `get_time_epoch`.
    """
    return self.get_time_epoch()
  
  
  def get_hb_utc(self, hb):
    """
    Generates a datetime object from a heartbeat and returns the UTC datetime.
    
    The algorithm is as follows:
    - get the remote timestamp from the heartbeat
    - get the remote timezone from the heartbeat
    - convert the remote timestamp to a datetime object
    - convert the remote datetime to UTC datetime by subtracting the offset hours
    - return the UTC datetime
    
    TODO:
    - add a check for the timezone format
    - add a check for the timestamp format    
    

    Parameters
    ----------
    hb : dict
      the hb object

    Returns
    -------
    datetime.datetime
    """
    ts = hb[ct.PAYLOAD_DATA.EE_TIMESTAMP]
    tz = hb.get(ct.PAYLOAD_DATA.EE_TIMEZONE, "UTC+0")        
    remote_datetime = datetime.strptime(ts, ct.HB.TIMESTAMP_FORMAT)
    offset_hours = int(tz.replace("UTC", ""))
    utc_datetime = remote_datetime - timedelta(hours=offset_hours)
    # the utc_datetime is naive so we need to add the timezone info
    utc_datetime = utc_datetime.replace(tzinfo=timezone.utc)
    return utc_datetime
  
  
  
  def __reset_timestamps(self, node_addr):
    """
    Resets the current epoch timestamps for a node.

    Parameters
    ----------
    node_addr : str
      The node address.
    """
    self.__data[node_addr][EPCT.LAST_EPOCH] = deepcopy(self.__data[node_addr][EPCT.CURRENT_EPOCH])
    self.__data[node_addr][EPCT.CURRENT_EPOCH][EPCT.HB_TIMESTAMPS] = set()
    self.__data[node_addr][EPCT.CURRENT_EPOCH][EPCT.ID] = self.get_time_epoch()
    return


  def __reset_all_timestamps(self):
    for node_addr in self.__data:
      self.__reset_timestamps(node_addr)
    return
  
  # FIXME: this method does not work as expected
  def __calculate_avail_seconds(self, timestamps, time_between_heartbeats=10):
    """
    This method calculates the availability of a node in the current epoch 
    based on the timestamps.

    Parameters
    ----------
    timestamps : set
      The set of timestamps for the current epoch.

    time_between_heartbeats: int
      Mandatory time between heartbeats in seconds.
    
    Returns
    -------
    int
      The availability seconds interval.
    """
    avail_seconds = 0
    nr_timestamps = len(timestamps)
    
    # need at least 2 hb timestamps to compute an interval 
    if nr_timestamps <= 1:
      return 0

    start_timestamp = timestamps[0]
    end_timestamp = timestamps[0]
    for i in range(1, nr_timestamps):
      # timestams should and must be sorted and in the same epoch
      delta = (timestamps[i] - timestamps[i - 1]).seconds
      # the delta between timestamps is bigger than the max heartbeat interval
      # or less than half the heartbeat interval (ignore same heartbeat)
      # TODO(AID): how can a heartbeat be sent more than once?
      # TODO: detect fraud mechanism (someone spams with heartbeats)
      if delta > (time_between_heartbeats + 5) or delta < (time_between_heartbeats / 2):
        # this gets triggered when the delta is too big or too small so last interval 
        # is considered invalid thus we compute up-to-last-valid interval availability
        # (ended with the last set of end_timestamp as end of interval
        avail_seconds += (end_timestamp - start_timestamp).seconds
        start_timestamp = timestamps[i]
      # endif delta

      # change the end of the current interval
      end_timestamp = timestamps[i]
    #endfor each hb timestamp

    # add the last interval length
    avail_seconds += (end_timestamp - start_timestamp).seconds
    return avail_seconds    


  def __calc_node_avail_seconds(self, node_addr, time_between_heartbeats=10, return_timestamps=False):
    if node_addr not in self.__data:
      self.__initialize_new_node(node_addr)
    # endif

    node_data = self.__data[node_addr]
    current_epoch_data = node_data[EPCT.CURRENT_EPOCH]
    timestamps = current_epoch_data[EPCT.HB_TIMESTAMPS]
    current_epoch = current_epoch_data[EPCT.ID]
    lst_timestamps = sorted(list(timestamps))
    avail_seconds = self.__calculate_avail_seconds(
      lst_timestamps, time_between_heartbeats=time_between_heartbeats
    )
    if return_timestamps:
      return avail_seconds, lst_timestamps, current_epoch
    return avail_seconds
    
  def get_current_epoch_availability(self, node_addr=None, time_between_heartbeats=10):
    # TODO: change this if we move to start-from-one offset
    epoch_start = self.__genesis_date + timedelta(
      seconds=(self.epoch_length * self.get_time_epoch()) # -1 if 1st epoch is genesis + length
    )
    max_possible_from_epoch_start = (self.get_current_date() - epoch_start).seconds

    if node_addr is None:
      node_addr = self.owner.node_addr
    # if node not seen yet, return None
    if node_addr not in self.__data:
      return None
    
    avail_seconds = self.__calc_node_avail_seconds(
      node_addr, 
      time_between_heartbeats=time_between_heartbeats
    )
    if max_possible_from_epoch_start == 0:
      prc_available = 0
    else:
      prc_available = round(avail_seconds / max_possible_from_epoch_start, 4)
    return prc_available


  def __recalculate_current_epoch_for_node(self, node_addr, time_between_heartbeats=10):
    """
    This method recalculates the current epoch availability for a node. 
    It should be used when the epoch changes just before resetting the timestamps.

    Parameters
    ----------
    node_addr : str
      The node address.
    """
    avail_seconds, lst_timestamps, current_epoch = self.__calc_node_avail_seconds(
      node_addr, time_between_heartbeats=time_between_heartbeats,
      return_timestamps=True
    )
    max_possible = self.epoch_length
    prc_available = round(avail_seconds / max_possible, 4) # DO NOT USE 100% but 1.0 
    record_value = round(prc_available * EPOCH_MAX_VALUE)
    self.__data[node_addr][EPCT.EPOCHS][current_epoch] = record_value
    
    if self.__debug:
      try:
        node_name = self.__data[node_addr][EPCT.NAME]
        node_name = node_name[:8]
        start_date, end_date = None, None
        if len(lst_timestamps) >= 1:
          start_date = self.date_to_str(lst_timestamps[0])
          end_date = self.date_to_str(lst_timestamps[-1])
        str_node_addr = node_addr[:8] + '...' + node_addr[-3:]
        self.P("{:<8}<{}> avail in ep {}: {} ({:.2f}%) from {} to {}".format(
          node_name, str_node_addr, current_epoch, 
          record_value, prc_available * 100, start_date, end_date
        ))
      except Exception as e:
        self.P("Error calculating availability for node: {}".format(node_addr), color='r')
        self.P(str(e), color='r')
    return prc_available, current_epoch


  def __recalculate_current_epoch_for_all(self):
    """
    This method recalculates the current epoch availability for all nodes using the recorded 
    timestamps.
    
    NOTE: this method should be called after the epoch has changed and the timestamps have been reset 
    within a critical section (mutex) as already done in `maybe_close_epoch`.
    """    
    self.P("Recalculating epoch {} availability for all nodes during epoch {}...".format(
      self.__current_epoch, self.get_time_epoch()
    ))

    # if current node was not 100% available, do not compute availability for other nodes
    self.start_timer('recalc_node_epoch')
    available_prc, current_epoch = self.__recalculate_current_epoch_for_node(
      self.owner.node_addr
    )
    self.stop_timer('recalc_node_epoch')
    # get the record value for the current node is actually redundant
    record_value = self.__data[self.owner.node_addr][EPCT.EPOCHS][current_epoch]
    
    # we can use available_prc or record_value to check if the current node >= SUPERVISOR_MIN_AVAIL
    # prc = available_prc is the same as record_value / EPOCH_MAX_VALUE
    prc = round(record_value / EPOCH_MAX_VALUE, 4) 
    was_up_throughout_current_epoch = prc >= SUPERVISOR_MIN_AVAIL_PRC

    if not was_up_throughout_current_epoch:
      msg = "Current node was {:.2f}% < {:.0f}%, available in epoch {} and so cannot compute " \
            "availability scores for other nodes".format(
              prc * 100, SUPERVISOR_MIN_AVAIL_PRC * 100, current_epoch
            )
      self.P(msg, color='r')
    else:
      self.start_timer('recalc_all_nodes_epoch')
      for node_addr in self.__data:
        self.start_timer('recalc_node_epoch')
        self.__recalculate_current_epoch_for_node(node_addr)
        self.stop_timer('recalc_node_epoch')
      self.stop_timer('recalc_all_nodes_epoch')
    # endif current node was not 100% available
    return


  def maybe_close_epoch(self):
    """
    This method checks if the current epoch has changed and if so, it closes the current epoch and 
    starts a new one. Closing the epoch implies recalculating the current epoch node availability 
    for all nodes and then resetting the timestamps.
    """
    result = 0 # assume no epoch change
    with self.log.managed_lock_resource(EPOCHMON_MUTEX):
      current_epoch = self.get_time_epoch()
      if self.__current_epoch is None:
        self.__current_epoch = current_epoch
        self.P("Starting epoch: {}".format(self.__current_epoch))
      elif current_epoch != self.__current_epoch:
        if current_epoch != (self.__current_epoch + 1):
          self.P("Epoch jump detected. Current epoch {} vs Last epoch {}".format(
            current_epoch, self.__current_epoch), color='r'
          )
        self.P("Closing epoch {} at start of epoch {}".format(self.__current_epoch, current_epoch))
        result = self.__current_epoch
        self.__recalculate_current_epoch_for_all()
        self.P("Starting epoch: {}".format(current_epoch))
        self.__current_epoch = current_epoch 
        self.__reset_all_timestamps()
        self.__save_status()  # save fresh status current epoch
        #endif epoch is not the same as the current one
      #endif current epoch is not None
    return result


  def __initialize_new_node(self, node_addr):
    name = self.get_node_name(node_addr)
    name = name[:8]
    node_name = self.get_node_name(node_addr)
    self.__data[node_addr] = _get_node_template(node_name)
    self.__reset_timestamps(node_addr)
    eth_node_addr = self.owner.node_address_to_eth_address(node_addr)
    self.__eth_to_node[eth_node_addr] = node_addr
    self.P("New node {:<8} <{}> / <{}> added to db".format(name, node_addr, eth_node_addr))
    return


  def register_data(self, node_addr, hb):
    """
    This method registers a heartbeat for a node in the current epoch.
    
    Parameters
    ----------
    node_addr : str
      The node address.
      
    hb : dict
      The heartbeat dict.
      
    """
    self.maybe_close_epoch()

    local_epoch = self.get_time_epoch()   
    # maybe first epoch for node_addr
    if node_addr not in self.__data:
      self.__initialize_new_node(node_addr)
    #endif node not in data
    dt_remote_utc = self.get_hb_utc(hb)
    str_date = self.date_to_str(dt_remote_utc)
    if self.__data[node_addr][EPCT.FIRST_SEEN] is None:
      self.__data[node_addr][EPCT.FIRST_SEEN] = str_date
    # check if the hb epoch is the same as the current one
    remote_epoch = self.get_epoch_id(dt_remote_utc)     
    if remote_epoch == local_epoch:
      # the remote epoch is the same as the local epoch so we can register the heartbeat
      with self.log.managed_lock_resource(EPOCHMON_MUTEX):
        # add the heartbeat timestamp for the current epoch
        self.__data[node_addr][EPCT.CURRENT_EPOCH][EPCT.HB_TIMESTAMPS].add(dt_remote_utc)
        self.__data[node_addr][EPCT.LAST_SEEN] = str_date
      # endwith lock
    else:
      self.P("Received invalid epoch {} from node {} on epoch {}".format(
        remote_epoch, node_addr, local_epoch
      ))
    #endif remote epoch is the same as the local epoch
    return
  
  
  def get_node_list(self):
    """
    Returns the list of nodes.
    """
    return list(self.data.keys())
  
  
  def get_node_state(self, node_addr):
    """
    Returns the state of a node in the current epoch.

    Parameters
    ----------
    node_addr : str
      The node address.
    """
    if node_addr not in self.__data:
      return None
    return self.__data[node_addr]
  
  
  def get_node_epochs(self, node_addr, autocomplete=True, as_list=False):
    """
    Returns the epochs availability for a node.

    Parameters
    ----------
    node_addr : str
      The node address.
      
    autocomplete : bool
      If True, the epochs are completed with 0 for missing epochs. Defaults to True in order to ensure continuity in the epochs data.
      
    as_list : bool
      If True, the epochs are returned as a list.
    """
    if node_addr not in self.__data:
      return None    
    dct_state = self.get_node_state(node_addr)
    dct_epochs = dct_state[EPCT.EPOCHS]
    current_epoch = self.get_time_epoch()
    epochs = list(range(1, current_epoch))
    if autocomplete or as_list:
      for epoch in epochs:
        if epoch not in dct_epochs:
          dct_epochs[epoch] = 0        
    lst_result = [dct_epochs.get(x, 0) for x in epochs]
    last_epochs = epochs[-5:]
    dct_last_epochs = {x : dct_epochs.get(x, 0) for x in last_epochs}
    non_zero = sum([1 for x in lst_result if x > 0])
    if self.__debug > 1:
      self.P("get_node_epochs({}), {} non zero, last epochs: {}".format(
        node_addr[:10] +'...' + node_addr[-4:], non_zero, str(dct_last_epochs)
      ))    
    if as_list:
      result = lst_result
    else:
      result = dct_epochs
    return result
  
  
  def get_node_last_n_epochs(self, node_addr, n=5, autocomplete=True, as_list=False):
    last_epoch = self.get_time_epoch() - 1
    dct_epochs = self.get_node_epochs(node_addr, autocomplete=autocomplete, as_list=False)
    start = max(1, last_epoch - n + 1)
    lst_epochs = list(range(start, last_epoch + 1))
    result = {x : dct_epochs.get(x, 0) for x in lst_epochs}
    if as_list:
      result = [result[x] for x in lst_epochs]
    return result
  
  
  
  def get_node_epoch(self, node_addr, epoch_id=None, as_percentage=False):
    """
    This method returns the percentage a node was alive in a given epoch.
    The data is returned from already calculated values.

    Parameters
    ----------
    node_addr : str
      The node address.
      
    epoch_id : int
      The epoch id. Defaults to the last epoch

    Returns
    -------
    float
      The value between 0 and 1 representing the percentage of the epoch the node was alive.
    """
    if node_addr not in self.__data:
      return 0
    if epoch_id is None:
      epoch_id = self.get_time_epoch() - 1
    if epoch_id < 1 or epoch_id >= self.get_time_epoch():
      raise ValueError("Invalid epoch requested: {}".format(epoch_id))
    # get the epochs data
    epochs = self.get_node_epochs(node_addr)
    if epochs is None:
      return 0    
    if as_percentage:
      return round(epochs[epoch_id] / 255, 4)
    return epochs[epoch_id]


  def get_node_previous_epoch(self, node_addr, as_percentage=False):
    """
    Returns the last epoch the node was alive.

    Parameters
    ----------
    node_addr : str
      The node address.
    """
    if node_addr not in self.__data:
      return 0
    last_epoch = self.get_time_epoch() - 1
    return self.get_node_epoch(node_addr, epoch_id=last_epoch, as_percentage=as_percentage)

  
  def get_node_last_epoch(self, node_addr, as_percentage=False):
    """
    Alias for get_node_previous_epoch.
    """
    return self.get_node_previous_epoch(node_addr, as_percentage=as_percentage)  


  def get_self_supervisor_capacity(self, as_float=False, start_epoch=None, end_epoch=None):
    """
    Returns the supervisor capacity for all the epochs
    
    Parameters
    ----------
    
    as_float : bool
      If True, the values are returned as floats. If False, the values are returned as bools
      based on (epochs[epoch] >= SUPERVISOR_MIN_AVAIL_UINT8).
      
    start_epoch : int
      The start epoch. Defaults to 1.
      
    end_epoch : int
      The end epoch. Defaults to the current epoch - 1.
      
    
    """
    epochs = self.get_node_epochs(self.owner.node_addr) or defaultdict(int)
    
    start_epoch = start_epoch if isinstance(start_epoch, int) else 1
    end_epoch = end_epoch if isinstance(end_epoch, int) else self.get_time_epoch() - 1
    
    lst_epochs = list(range(start_epoch, end_epoch + 1))
    
    result = {
      epoch : 
        (epochs[epoch] >= SUPERVISOR_MIN_AVAIL_UINT8) if not as_float else
        (round(epochs[epoch] / EPOCH_MAX_VALUE,2))
      for epoch in lst_epochs
    }
    return result
    

  def get_node_first_epoch(self, node_addr):
    """
    Returns the first epoch the node was alive.

    Parameters
    ----------
    node_addr : str
      The node address.
    """
    if node_addr not in self.__data:
      return -1
    epochs = list(self.get_node_epochs(node_addr).keys())
    min_epoch = min(epochs)
    return min_epoch
  
  

  def get_era_specs(self):
    """
    This function returns in human readable format the era specifications meaning it will return:
    - the current epoch
    - the current date
    - genesis date
    - epoch intervals
    - epoch interval seconds
    """
    dct_result = {
      'current_epoch' : self.get_current_epoch(),
      'current_date' : self.date_to_str(self.get_current_date()),
      'genesis_date' : self.__genesis_date_str,
      'epoch_intervals' : self.__epoch_intervals,
      'epoch_interval_seconds' :  self.__epoch_interval_seconds,
      'epoch_length' : self.epoch_length,
    }
    return dct_result


  def get_oracle_state(
    self, display=False, 
    start_epoch=None, end_epoch=None,
    as_int=False,
  ):
    """
    Returns the server/oracle state.
    """
    dct_result = self.get_era_specs()
    if start_epoch is None:
      start_epoch = max(1, self.get_current_epoch() - 10)
    certainty = self.get_self_supervisor_capacity(
      as_float=True, start_epoch=start_epoch, end_epoch=end_epoch,
    )
    epochs = sorted(list(certainty.keys()))
    certainty_int = {
      x : int(certainty[x] >= SUPERVISOR_MIN_AVAIL_PRC) for x in epochs
    }
    if as_int:
      certainty = certainty_int
    dct_result['manager'] = {
      'certainty' : certainty, 
    }
    dct_result['manager']['valid'] = sum(certainty_int.values()) == len(certainty)
    dct_result['manager']['supervisor_min_avail_prc'] = SUPERVISOR_MIN_AVAIL_PRC
    for extra_key in _FULL_DATA_INFO_KEYS:
      dct_result['manager'][extra_key.lower()] = self.__full_data.get(extra_key, 'N/A')
    if (time() - self.__last_state_log) > 600:
      display = True
    if display:
      self.P("Oracle state:\n{}".format(json.dumps(dct_result, indent=2)))
      self.__last_state_log = time()
    return dct_result


  def get_stats(self, display=True, online_only=False):
    """
    Returns the overall statistics for all nodes.
    """
    
    stats = {}
    best_avail = 0
    NR_HIST = 10
    nr_eps = 0   
    saves = self.__full_data.get(SYNC_SAVES_TS, 'N/A')
    saves_epoch = self.__full_data.get(SYNC_SAVES_EP, 'N/A')
    restarts = self.__full_data.get(SYNC_RESTARTS, 'N/A')
    current_epoch = self.get_current_epoch()
    start_epoch = max(1, current_epoch - NR_HIST)
    certainty = self.get_self_supervisor_capacity(as_float=True, start_epoch=start_epoch)
    oracle_state = self.get_oracle_state()      
    for node_addr in self.data:
      is_online = self.owner.network_node_is_online(
        node_addr, dt_now=self.get_current_date()
      )
      if online_only and not is_online:
          continue
      dt_netmon_last_seen = self.owner.network_node_last_seen(
        node_addr, 
        dt_now=self.get_current_date(),
        as_sec=False
      )
      last_seen_ago = self.owner.network_node_last_seen(
        node_addr, 
        dt_now=self.get_current_date(),
        as_sec=True
      )
      netmon_last_seen = self.date_to_str(dt_netmon_last_seen) if dt_netmon_last_seen is not None else 'N/A'
      node_name = self.get_node_name(node_addr)
      dct_epochs = self.get_node_epochs(node_addr, as_list=False, autocomplete=True)     
      
      # process the previous epoch hb data
      node_last_epoch_data = self.data[node_addr][EPCT.LAST_EPOCH]
      node_last_epoch_id = node_last_epoch_data[EPCT.ID]
      node_last_epoch_hb_timestamps = node_last_epoch_data[EPCT.HB_TIMESTAMPS]
      node_last_epoch_hb_timestamps = sorted(list(node_last_epoch_hb_timestamps))
      node_last_epoch_1st_hb = node_last_epoch_hb_timestamps[0] if len(node_last_epoch_hb_timestamps) > 0 else None
      node_last_epoch_1st_hb = self.date_to_str(node_last_epoch_1st_hb)
      node_last_epoch_last_hb = node_last_epoch_hb_timestamps[-1] if len(node_last_epoch_hb_timestamps) > 0 else None
      node_last_epoch_last_hb = self.date_to_str(node_last_epoch_last_hb)
      node_last_epoch_nr_hb = len(node_last_epoch_hb_timestamps)
      node_last_epoch_avail = round(
        self.__calculate_avail_seconds(node_last_epoch_hb_timestamps) / self.epoch_length, 4
      )
      
      node_current_epoch_data = self.data[node_addr][EPCT.CURRENT_EPOCH]
      node_current_epoch_id = node_current_epoch_data[EPCT.ID]
      node_current_epoch_hb_timestamps = node_current_epoch_data[EPCT.HB_TIMESTAMPS]
      node_current_epoch_hb_timestamps = sorted(list(node_current_epoch_hb_timestamps))
      node_current_epoch_1st_hb = node_current_epoch_hb_timestamps[0] if len(node_current_epoch_hb_timestamps) > 0 else None
      node_current_epoch_last_hb = node_current_epoch_hb_timestamps[-1] if len(node_current_epoch_hb_timestamps) > 0 else None
      node_current_epoch_nr_hb = len(node_current_epoch_hb_timestamps)
      
      
      epochs_ids = sorted(list(dct_epochs.keys()))
      epochs = [dct_epochs[x] for x in epochs_ids]
      selection = epochs_ids[-NR_HIST:]
      str_last_epochs = str({x : dct_epochs.get(x, 0) for x in selection})
      str_certainty =  ", ".join([
        f"{x}={'Y' if certainty.get(x, 0) >= SUPERVISOR_MIN_AVAIL_PRC else 'N'}" 
        for x in selection
      ])    
      MAX_AVAIL = EPOCH_MAX_VALUE * len(epochs) # max avail possible for this node
      score = sum(epochs)      
      avail = round(score / MAX_AVAIL, 4)
      best_avail = max(best_avail, avail)
      non_zero = len([x for x in epochs if x > 0])
      nr_eps = len(epochs)
      prev_epoch = self.get_time_epoch() - 1
      first_seen = self.data[node_addr][EPCT.FIRST_SEEN]
      last_seen = self.data[node_addr][EPCT.LAST_SEEN]
      eth_addr = self.owner.node_address_to_eth_address(node_addr)
      if nr_eps != prev_epoch:
        raise ValueError("Epochs mismatch for node: {} - total {} vs prev {}".format(
          node_addr, nr_eps, prev_epoch
        ))
      stats[node_addr] = {
        'eth_addr' : eth_addr,
        'alias' : node_name,
        'last_state' : netmon_last_seen,
        'last_seen_ago' : self.log.elapsed_to_str(last_seen_ago),
        'non_zero' : non_zero,
        'overall_availability' : avail,
        'score' : score,
        'first_check' : first_seen,
        'last_check' : last_seen,
        
        'current_epoch' : {
          'nr_hb' : node_current_epoch_nr_hb,
          '1st_hb' : str(node_current_epoch_1st_hb),
          'last_hb' : str(node_current_epoch_last_hb),
        },
        
        'recent_history' : {
          'last_10_ep' : str_last_epochs,
          'certainty' : str_certainty,
          'last_epoch_id' : node_last_epoch_id,
          'last_epoch_nr_hb' : node_last_epoch_nr_hb,
          'last_epoch_1st_hb' : node_last_epoch_1st_hb,
          'last_epoch_last_hb' : node_last_epoch_last_hb,
          'last_epoch_avail' : node_last_epoch_avail,
        }
      }
      if node_addr == self.owner.node_addr:
        stats[node_addr]['oracle'] = oracle_state
      #endif node is current node
    #endfor each node
    if display:
      str_stats = json.dumps(stats, indent=2)
      self.P("EpochManager report at ep {} (max_score: {}, nr_eps: {}):\nRecent saves: {}\nRecent saves epochs: {}\nRecent restars: {}\nOracle info:\n{}\n\nStatuses:\n{}".format(
        current_epoch, best_avail, nr_eps,
        saves, saves_epoch, restarts, 
        json.dumps(oracle_state, indent=2),
        str_stats
      ))
    return stats
  

### Below area contains the methods for availability resulted from multi-oracle sync

  def get_last_sync_epoch(self):
    """
    Returns the last sync epoch.

    Returns
    -------
    int
      The last sync epoch.
    """
    return self.__full_data[SYNC_LAST_EPOCH]


  def get_epoch_availability(self, epoch):
    """
    Returns the availability table for a given epoch.

    Parameters
    ----------
    epoch : int
      The epoch id.

    Returns
    -------
    dict
      The availability table for the specified epoch.
    """

    availability_table = {}

    for node_addr in self.__data:
      epochs: defaultdict = self.get_node_epochs(node_addr, as_list=False)
      availability_table[node_addr] = {
        SYNC_VALUE : epochs.get(epoch, 0),
        SYNC_SIGNATURES : self.__data[node_addr][EPCT.SIGNATURES].get(epoch, [])
      }
    # end for each node

    return availability_table


  def update_epoch_availability(self, epoch, availability_table):
    """
    Updates the epoch availability for a given epoch.

    !! IMPORTANT !!
    ---------------
    Make sure the epoch is strictly greater than the last sync epoch.
    It is ideal that this method is called with `epoch == last_sync_epoch + 1`.

    Parameters
    ----------
    epoch : int
      The epoch id.

    availability_table : dict
      The availability table.
    """
    last_sync_epoch = self.get_last_sync_epoch()

    assert epoch > last_sync_epoch, \
      f"Epoch {epoch} is not greater than last sync epoch {last_sync_epoch}"

    for node_addr in availability_table:
      if node_addr not in self.__data:
        self.__initialize_new_node(node_addr)
      self.__data[node_addr][EPCT.EPOCHS][epoch] = availability_table[node_addr][SYNC_VALUE]
      self.__data[node_addr][EPCT.SIGNATURES][epoch] = availability_table[node_addr][SYNC_SIGNATURES]
    self.__full_data[SYNC_LAST_EPOCH] = epoch

    return




if __name__ == '__main__':
  from naeural_core.core_logging import Logger
  from naeural_core.main.net_mon import NetworkMonitor
  
  FN_NETWORK = r"_local_cache\_data\network_monitor\db.pkl"
  
  EPOCH_MANAGER_DEBUG = False
  
  l = Logger('EPOCH', base_folder='.', app_folder='_local_cache')
  
  DATES = [
    '2024-07-08 12:00:00',
    '2024-07-07 12:00:00',
    '2024-07-08 12:00:00',
    '2024-07-09 12:00:00',
    '2024-07-10 12:00:00',
  ]
  
  NODES = [
    '0xai_AkyWQ91tdk0QdJfH70nmRG6euFjxwYf1FSC7mBdtIbTh',
    '0xai_AgNxIxNN6RsDqBa0d5l2ZQpy7y-5bnbP55xej4OvcitO',
  ]
  
  # make sure you have a recent (today) save network status
  # eng1 = EpochsManager(log=l, owner=1234, debug_date=DATES[0], debug=True)
  # eng2 = EpochsManager(log=l, owner=None, debug_date=DATES[1])
  # assert id(eng1) == id(eng2)
  
  PREDEFINED_TESTS = {
    'aid_01' : {
      'date' :'2025-01-24 09:07:00',
      'addr' : '0xai_AleLPKqUHV-iPc-76-rUvDkRWW4dFMIGKW1xFVcy65nH'
    },
    'nen-2' : {
      'date' :'2025-01-24 11:26:00',
      'addr' : '0xai_Amfnbt3N-qg2-qGtywZIPQBTVlAnoADVRmSAsdDhlQ-6'      
    }
  }
  
  CHOSEN_TEST = 'nen-2'
  
  CURRENT_DATE = PREDEFINED_TESTS[CHOSEN_TEST]['date']
  NODE_ADDR = PREDEFINED_TESTS[CHOSEN_TEST]['addr']
  
  if True:
    netmon = NetworkMonitor(
      log=l, node_name=CHOSEN_TEST, node_addr=NODE_ADDR,
      # epoch_manager=eng
    )
  else:
    netmon = NetworkMonitor(
      log=l, node_name='aid_hpc', node_addr='0xai_AgNxIxNN6RsDqBa0d5l2ZQpy7y-5bnbP55xej4OvcitO'
    )
    
  # eng.owner = netmon
  eng = netmon.epoch_manager
  
  
  TEST_A = False
  TEST_B = True
  
  if TEST_A:
    assert id(eng) == id(netmon.epoch_manager)  

    has_data = netmon.network_load_status(FN_NETWORK)
    
    if has_data:    
      l.P("Current time epoch is: {} ({})".format(eng.get_time_epoch(), eng.epoch_to_date()))
      
      nodes = netmon.all_nodes
          
      dct_hb = {}
      
      # now check the nodes for some usable data
      _current_epoch = eng.get_time_epoch()
      for node_addr in nodes:
        hbs = netmon.get_box_heartbeats(node_addr)
        idx = -1
        done = False
        good_hbs = defaultdict(list)
        for hb in hbs:
          ep = eng.get_epoch_id(hb[ct.PAYLOAD_DATA.EE_TIMESTAMP])
          if ep >= _current_epoch:
            good_hbs[ep].append(hb)
        if len(good_hbs) > 0:
          dct_hb[node_addr] = good_hbs
      
      l.P("Data available for epochs:\n{}".format(
        "\n".join(["{}: {}".format(x, list(dct_hb[x].keys())) for x in dct_hb]) 
      ))
      
      
      for step in range(5):
        current_date = DATES[step]
        eng._set_dbg_date(current_date)
        epoch = eng.get_epoch_id(current_date)
        l.P("Running step {} - epoch {} / {}".format(
          step, epoch, current_date), color='b'
        )
        epoch_has_data = any([epoch in dct_hb[x] for x in dct_hb])
        if epoch_has_data:
          l.P("Starting registering data for epoch {}...".format(eng.get_current_epoch()), color='b')
        data_counter = 0
        for node_addr in dct_hb:
          for hb in dct_hb[node_addr][epoch]:
            eng.register_data(node_addr, hb)
            data_counter += 1
        if data_counter > 0:
          l.P("Data loaded ({}) for epoch {}.".format(
            data_counter, eng.get_current_epoch()), color='g'
          )
        else:
          l.P("No data registered for epoch {}.".format(eng.get_current_epoch()), color='r')
        #endif had data
      #endfor each step
      final_date = DATES[-1]
      l.P("Done all steps, setting final date: {}".format(final_date), color='b')
      eng._set_dbg_date(final_date)    
      eng.maybe_close_epoch()
      
      l.P('{}: {}'.format(
        eng.get_node_name(NODES[-2]), eng.get_node_epochs(NODES[-2], as_list=True))
      )
      l.P('{}: {}'.format(
        eng.get_node_name(NODES[-1]), eng.get_node_epochs(NODES[-1], as_list=True))
      )    
  #endif TEST_A
  
  if TEST_B:
    str_date = CURRENT_DATE
    debug_date = l.str_to_date(str_date) # get date
    debug_date = debug_date.astimezone(timezone.utc) # convert to UTC
    eng._set_dbg_date(debug_date)
    inf = eng.get_stats(display=True, online_only=True)
    m, t, top = l.get_obj_size(obj=netmon.all_heartbeats, top_consumers=20, return_tree=True)
    # l.P("Heartbeats size: {:,.0f} MB".format(m / 1024 / 1024))
    eng.get_current_epoch_availability(NODE_ADDR)
  