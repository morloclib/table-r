import signal
import sys
import select
import os # required for setting path to morloc dependencies
import time
import copy
import array
import struct
import socket as _socket
from collections import OrderedDict
from multiprocessing import Process, Value, RawValue
import ctypes
import functools


# Global variables for clean signal handling
daemon = None
workers = []
global_state = dict()
_shutdown_wakeup_fd = -1

# AUTO include sources start
sys.path = [os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..")), os.path.expanduser("."), os.path.expanduser("/home/dev/.local/share/morloc/opt"), os.path.expanduser("/home/dev/.local/share/morloc/src/morloc/plane")] + sys.path
import importlib
import pymorloc as morloc
default_table_test_table_test = importlib.import_module("default.table.test.table-test")
mlc_schema_table = [ "T:11x<int>j"
, "<int>j"
, "T:21x<int>j1y<str>s"
, "T:11a<int>j"
, "<list>a:3<int>j"
, "T:11y<str>s"
, "T:31x<int>j1y<str>s1z<float>f8"
, "<list>a:3<str>s"
, "<list>a:3<float>f8"
, "<list>a:6<int>j"
, "<list>a:1<int>j"
, "<list>a:3<bool>b"
, "<list>a:2<int>j"
, "<list>a<str>s"
, "<tuple>t2<int>j<int>j" ]
# AUTO include sources end

# Dynamic worker spawning: monkey-patch foreign_call to track busy workers.
# Workers atomically increment busy_count before a foreign_call and decrement
# after. When busy_count reaches total_workers, a byte is written to a wake-up
# pipe to tell the main process to spawn a new worker.
_original_foreign_call = morloc.foreign_call
_busy_ref = None
_total_ref = None
_wakeup_fd = -1

def _init_worker_tracking(busy, total, wakeup_fd):
    global _busy_ref, _total_ref, _wakeup_fd
    _busy_ref = busy
    _total_ref = total
    _wakeup_fd = wakeup_fd
    morloc.foreign_call = _tracked_foreign_call

def _tracked_foreign_call(*args):
    prev = _busy_ref.value
    _busy_ref.value = prev + 1
    if prev + 1 >= _total_ref.value and _wakeup_fd >= 0:
        try:
            os.write(_wakeup_fd, b'!')
        except OSError:
            pass
    try:
        return _original_foreign_call(*args)
    finally:
        _busy_ref.value -= 1

# AUTO include manifolds start
def m2893(s38):
    try:
        s39 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-r")
        , 2893
        , [s38] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m2893):\n{e!s}")
    return(morloc.get_value(s39, mlc_schema_table[0]))

def m2898(s35):
    try:
        s40 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-r")
        , 2898
        , [s35] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m2898):\n{e!s}")
    return(morloc.get_value(s40, mlc_schema_table[0]))

def m2907(s38):
    try:
        s41 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-r")
        , 2907
        , [s38] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m2907):\n{e!s}")
    return(morloc.get_value(s41, mlc_schema_table[0]))

def m2919(s36):
    try:
        s42 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-r")
        , 2919
        , [s36] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m2919):\n{e!s}")
    return(morloc.get_value(s42, mlc_schema_table[0]))

def m2926(s38):
    try:
        s43 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-r")
        , 2926
        , [s38] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m2926):\n{e!s}")
    return(morloc.get_value(s43, mlc_schema_table[0]))

def m2940(s37):
    try:
        s44 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-r")
        , 2940
        , [s37] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m2940):\n{e!s}")
    return(morloc.get_value(s44, mlc_schema_table[0]))

def m3001():
    try:
        s47 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-r")
        , 3001
        , [] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3001):\n{e!s}")
    return(morloc.get_value(s47, mlc_schema_table[1]))

def m3017():
    try:
        s48 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-r")
        , 3017
        , [] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3017):\n{e!s}")
    return(morloc.get_value(s48, mlc_schema_table[0]))

def m3031():
    try:
        s49 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-r")
        , 3031
        , [] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3031):\n{e!s}")
    return(morloc.get_value(s49, mlc_schema_table[1]))

def m3045():
    try:
        s50 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-r")
        , 3045
        , [] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3045):\n{e!s}")
    return(morloc.get_value(s50, mlc_schema_table[2]))

def m3053():
    try:
        s51 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-r")
        , 3053
        , [] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3053):\n{e!s}")
    return(morloc.get_value(s51, mlc_schema_table[2]))

def m3187():
    try:
        s52 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-r")
        , 3187
        , [] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3187):\n{e!s}")
    return(morloc.get_value(s52, mlc_schema_table[3]))

def m3194():
    try:
        s53 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-r")
        , 3194
        , [] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3194):\n{e!s}")
    return(morloc.get_value(s53, mlc_schema_table[3]))

def m3201():
    try:
        s54 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-r")
        , 3201
        , [] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3201):\n{e!s}")
    return(morloc.get_value(s54, mlc_schema_table[4]))

def m3268():
    try:
        s56 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-r")
        , 3268
        , [] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3268):\n{e!s}")
    return(morloc.get_value(s56, mlc_schema_table[0]))

def m3273():
    try:
        s57 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-r")
        , 3273
        , [] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3273):\n{e!s}")
    return(morloc.get_value(s57, mlc_schema_table[0]))

def m3279():
    try:
        s58 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-r")
        , 3279
        , [] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3279):\n{e!s}")
    return(morloc.get_value(s58, mlc_schema_table[1]))

def m3375():
    try:
        s59 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-r")
        , 3375
        , [] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3375):\n{e!s}")
    return(morloc.get_value(s59, mlc_schema_table[0]))

def m3382():
    try:
        s60 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-r")
        , 3382
        , [] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3382):\n{e!s}")
    return(morloc.get_value(s60, mlc_schema_table[0]))

def m3390():
    try:
        s61 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-r")
        , 3390
        , [] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3390):\n{e!s}")
    return(morloc.get_value(s61, mlc_schema_table[5]))

def m3397():
    try:
        s62 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-r")
        , 3397
        , [] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3397):\n{e!s}")
    return(morloc.get_value(s62, mlc_schema_table[5]))

def m3403():
    try:
        s63 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-r")
        , 3403
        , [] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3403):\n{e!s}")
    return(morloc.get_value(s63, mlc_schema_table[2]))

def m3409():
    try:
        s64 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-r")
        , 3409
        , [] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3409):\n{e!s}")
    return(morloc.get_value(s64, mlc_schema_table[2]))

def m3541():
    try:
        s65 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-r")
        , 3541
        , [] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3541):\n{e!s}")
    return(morloc.get_value(s65, mlc_schema_table[2]))

def m3546():
    try:
        s66 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-r")
        , 3546
        , [] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3546):\n{e!s}")
    return(morloc.get_value(s66, mlc_schema_table[2]))

def m3554():
    try:
        s67 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-r")
        , 3554
        , [] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3554):\n{e!s}")
    return(morloc.get_value(s67, mlc_schema_table[5]))

def m3560():
    try:
        s68 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-r")
        , 3560
        , [] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3560):\n{e!s}")
    return(morloc.get_value(s68, mlc_schema_table[5]))

def m3566():
    try:
        s69 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-r")
        , 3566
        , [] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3566):\n{e!s}")
    return(morloc.get_value(s69, mlc_schema_table[0]))

def m3573():
    try:
        s70 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-r")
        , 3573
        , [] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3573):\n{e!s}")
    return(morloc.get_value(s70, mlc_schema_table[0]))

def m3721():
    try:
        s74 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-r")
        , 3721
        , [] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3721):\n{e!s}")
    return(morloc.get_value(s74, mlc_schema_table[2]))

def m3728():
    try:
        s75 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-r")
        , 3728
        , [] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3728):\n{e!s}")
    return(morloc.get_value(s75, mlc_schema_table[2]))

def m3736():
    try:
        s76 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-r")
        , 3736
        , [] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3736):\n{e!s}")
    return(morloc.get_value(s76, mlc_schema_table[6]))

def m3743():
    try:
        s77 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-r")
        , 3743
        , [] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3743):\n{e!s}")
    return(morloc.get_value(s77, mlc_schema_table[6]))

def m3901():
    try:
        s78 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-r")
        , 3901
        , [] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3901):\n{e!s}")
    return(morloc.get_value(s78, mlc_schema_table[4]))

def m3913():
    try:
        s80 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-r")
        , 3913
        , [] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3913):\n{e!s}")
    return(morloc.get_value(s80, mlc_schema_table[7]))

def m3921():
    try:
        s82 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-r")
        , 3921
        , [] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3921):\n{e!s}")
    return(morloc.get_value(s82, mlc_schema_table[8]))

def m4036():
    try:
        s85 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-r")
        , 4036
        , [] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4036):\n{e!s}")
    return(morloc.get_value(s85, mlc_schema_table[0]))

def m4045():
    try:
        s86 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-r")
        , 4045
        , [] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4045):\n{e!s}")
    return(morloc.get_value(s86, mlc_schema_table[0]))

def m4053(s9):
    try:
        s87 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-r")
        , 4053
        , [s9] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4053):\n{e!s}")
    return(morloc.get_value(s87, mlc_schema_table[0]))

def m4063():
    try:
        s88 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-r")
        , 4063
        , [] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4063):\n{e!s}")
    return(morloc.get_value(s88, mlc_schema_table[0]))

def m4071():
    try:
        s89 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-r")
        , 4071
        , [] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4071):\n{e!s}")
    return(morloc.get_value(s89, mlc_schema_table[0]))

def m4080(s9):
    try:
        s90 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-r")
        , 4080
        , [s9] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4080):\n{e!s}")
    return(morloc.get_value(s90, mlc_schema_table[0]))

def m4087():
    try:
        s91 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-r")
        , 4087
        , [] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4087):\n{e!s}")
    return(morloc.get_value(s91, mlc_schema_table[0]))

def m4093():
    try:
        s92 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-r")
        , 4093
        , [] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4093):\n{e!s}")
    return(morloc.get_value(s92, mlc_schema_table[0]))

def m4221():
    try:
        s100 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-r")
        , 4221
        , [] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4221):\n{e!s}")
    return(morloc.get_value(s100, mlc_schema_table[0]))

def m4226():
    try:
        s101 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-r")
        , 4226
        , [] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4226):\n{e!s}")
    return(morloc.get_value(s101, mlc_schema_table[0]))

def m4234(s94):
    try:
        s102 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-r")
        , 4234
        , [s94] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4234):\n{e!s}")
    return(morloc.get_value(s102, mlc_schema_table[0]))

def m4237(s99):
    try:
        s103 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-r")
        , 4237
        , [s99] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4237):\n{e!s}")
    return(morloc.get_value(s103, mlc_schema_table[0]))

def m4244(s96):
    try:
        s104 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-r")
        , 4244
        , [s96] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4244):\n{e!s}")
    return(morloc.get_value(s104, mlc_schema_table[0]))

def m4333(s17):
    try:
        s112 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-r")
        , 4333
        , [s17] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4333):\n{e!s}")
    return(morloc.get_value(s112, mlc_schema_table[0]))

def m4339():
    try:
        s113 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-r")
        , 4339
        , [] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4339):\n{e!s}")
    return(morloc.get_value(s113, mlc_schema_table[0]))

def m4347(s18):
    try:
        s114 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-r")
        , 4347
        , [s18] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4347):\n{e!s}")
    return(morloc.get_value(s114, mlc_schema_table[0]))

def m4353(s21):
    try:
        s115 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-r")
        , 4353
        , [s21] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4353):\n{e!s}")
    return(morloc.get_value(s115, mlc_schema_table[0]))

def m4362(s19):
    try:
        s116 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-r")
        , 4362
        , [s19] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4362):\n{e!s}")
    return(morloc.get_value(s116, mlc_schema_table[0]))

def m4368(s22):
    try:
        s117 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-r")
        , 4368
        , [s22] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4368):\n{e!s}")
    return(morloc.get_value(s117, mlc_schema_table[0]))

def m4375(s20):
    try:
        s118 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-r")
        , 4375
        , [s20] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4375):\n{e!s}")
    return(morloc.get_value(s118, mlc_schema_table[0]))

def m4381(s23):
    try:
        s119 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-r")
        , 4381
        , [s23] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4381):\n{e!s}")
    return(morloc.get_value(s119, mlc_schema_table[0]))

def m4488(s24):
    try:
        s124 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-r")
        , 4488
        , [s24] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4488):\n{e!s}")
    return(morloc.get_value(s124, mlc_schema_table[0]))

def m4494():
    try:
        s125 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-r")
        , 4494
        , [] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4494):\n{e!s}")
    return(morloc.get_value(s125, mlc_schema_table[0]))

def m4502(s25):
    try:
        s126 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-r")
        , 4502
        , [s25] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4502):\n{e!s}")
    return(morloc.get_value(s126, mlc_schema_table[1]))

def m4516(s26):
    try:
        s127 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-r")
        , 4516
        , [s26] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4516):\n{e!s}")
    return(morloc.get_value(s127, mlc_schema_table[0]))

def m4522(s27):
    try:
        s128 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-r")
        , 4522
        , [s27] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4522):\n{e!s}")
    return(morloc.get_value(s128, mlc_schema_table[0]))

def m4529(s25):
    try:
        s129 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-r")
        , 4529
        , [s25] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4529):\n{e!s}")
    return(morloc.get_value(s129, mlc_schema_table[13]))

def m4646(s137):
    try:
        s138 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-r")
        , 4646
        , [s137] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4646):\n{e!s}")
    return(morloc.get_value(s138, mlc_schema_table[1]))

def m4659(s137):
    try:
        s139 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-r")
        , 4659
        , [s137] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4659):\n{e!s}")
    return(morloc.get_value(s139, mlc_schema_table[13]))

def m4673(s137):
    try:
        s141 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-r")
        , 4673
        , [s137] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4673):\n{e!s}")
    return(morloc.get_value(s141, mlc_schema_table[0]))

def m4678(s131):
    try:
        s142 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-r")
        , 4678
        , [s131] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4678):\n{e!s}")
    return(morloc.get_value(s142, mlc_schema_table[0]))

def m4687(s137):
    try:
        s143 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-r")
        , 4687
        , [s137] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4687):\n{e!s}")
    return(morloc.get_value(s143, mlc_schema_table[0]))

def m4692(s134):
    try:
        s144 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-r")
        , 4692
        , [s134] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4692):\n{e!s}")
    return(morloc.get_value(s144, mlc_schema_table[0]))

def m4701(s137):
    try:
        s145 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-r")
        , 4701
        , [s137] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4701):\n{e!s}")
    return(morloc.get_value(s145, mlc_schema_table[0]))

def m4706(s136):
    try:
        s146 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-r")
        , 4706
        , [s136] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4706):\n{e!s}")
    return(morloc.get_value(s146, mlc_schema_table[0]))

def m4715(s137):
    try:
        s147 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-r")
        , 4715
        , [s137] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4715):\n{e!s}")
    return(morloc.get_value(s147, mlc_schema_table[1]))

def m4728(s137):
    try:
        s148 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-r")
        , 4728
        , [s137] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4728):\n{e!s}")
    return(morloc.get_value(s148, mlc_schema_table[1]))

def m4741(s137):
    try:
        s149 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-r")
        , 4741
        , [s137] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4741):\n{e!s}")
    return(morloc.get_value(s149, mlc_schema_table[0]))

def m4746(s132):
    try:
        s150 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-r")
        , 4746
        , [s132] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4746):\n{e!s}")
    return(morloc.get_value(s150, mlc_schema_table[0]))

def m4755(s137):
    try:
        s151 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-r")
        , 4755
        , [s137] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4755):\n{e!s}")
    return(morloc.get_value(s151, mlc_schema_table[0]))

def m4760(s135):
    try:
        s152 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-r")
        , 4760
        , [s135] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4760):\n{e!s}")
    return(morloc.get_value(s152, mlc_schema_table[0]))

def m4769(s137):
    try:
        s153 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-r")
        , 4769
        , [s137] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4769):\n{e!s}")
    return(morloc.get_value(s153, mlc_schema_table[0]))

def m4774(s135):
    try:
        s154 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-r")
        , 4774
        , [s135] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4774):\n{e!s}")
    return(morloc.get_value(s154, mlc_schema_table[0]))

def m4783(s137):
    try:
        s155 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-r")
        , 4783
        , [s137] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4783):\n{e!s}")
    return(morloc.get_value(s155, mlc_schema_table[0]))

def m4788(s133):
    try:
        s156 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-r")
        , 4788
        , [s133] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4788):\n{e!s}")
    return(morloc.get_value(s156, mlc_schema_table[0]))

def m4797(s137):
    try:
        s157 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-r")
        , 4797
        , [s137] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4797):\n{e!s}")
    return(morloc.get_value(s157, mlc_schema_table[1]))

def m4808(s137):
    try:
        s158 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-r")
        , 4808
        , [s137] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4808):\n{e!s}")
    return(morloc.get_value(s158, mlc_schema_table[1]))

def m4921():
    try:
        s159 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-r")
        , 4921
        , [] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4921):\n{e!s}")
    return(morloc.get_value(s159, mlc_schema_table[13]))

def m4931():
    try:
        s161 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-r")
        , 4931
        , [] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4931):\n{e!s}")
    return(morloc.get_value(s161, mlc_schema_table[13]))

def m4995():
    try:
        s163 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-r")
        , 4995
        , [] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4995):\n{e!s}")
    return(morloc.get_value(s163, mlc_schema_table[0]))

def m4999():
    try:
        s164 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-r")
        , 4999
        , [] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4999):\n{e!s}")
    return(morloc.get_value(s164, mlc_schema_table[0]))

def m5007():
    try:
        s165 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-r")
        , 5007
        , [] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m5007):\n{e!s}")
    return(morloc.get_value(s165, mlc_schema_table[5]))

def m5014():
    try:
        s166 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-r")
        , 5014
        , [] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m5014):\n{e!s}")
    return(morloc.get_value(s166, mlc_schema_table[5]))

def m5022():
    try:
        s167 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-r")
        , 5022
        , [] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m5022):\n{e!s}")
    return(morloc.get_value(s167, mlc_schema_table[1]))

def m5031():
    try:
        s168 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-r")
        , 5031
        , [] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m5031):\n{e!s}")
    return(morloc.get_value(s168, mlc_schema_table[1]))

def m4987():
    try:
        s169 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-r")
        , 2739
        , [] )
        n170 = default_table_test_table_test.printMsg( "asCol / nrow / ncol"
        , morloc.get_value(s169, mlc_schema_table[14]) )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4987):\n{e!s}")
    return(n170)

def m5072():
    try:
        n171 = default_table_test_table_test.testEqual( "ncol (mkXYZ3) = 3"
        , m5031()
        , 3
        , m4987() )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m5072):\n{e!s}")
    return(n171)

def m5057():
    try:
        n172 = default_table_test_table_test.testEqual( "nrow (mkX 3) = 3"
        , m5022()
        , 3
        , m5072() )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m5057):\n{e!s}")
    return(n172)

def m5038():
    try:
        n173 = default_table_test_table_test.testEqual( "asCol from List literal"
        , m5007()
        , m5014()
        , m5057() )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m5038):\n{e!s}")
    return(n173)

def m4900():
    try:
        n174 = default_table_test_table_test.testEqual( "asCol \"x\" xs3 == mkX 3"
        , m4995()
        , m4999()
        , m5038() )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4900):\n{e!s}")
    return(n174)

def m4913():
    try:
        n175 = default_table_test_table_test.printMsg("names", m4900())
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4913):\n{e!s}")
    return(n175)

def m4941():
    try:
        n162 = ["x", "y", "z"]
        n176 = default_table_test_table_test.testEqual( "names mkXYZ3 = [\"x\", \"y\", \"z\"]"
        , m4931()
        , n162
        , m4913() )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4941):\n{e!s}")
    return(n176)

def m4605():
    try:
        n160 = ["x"]
        n177 = default_table_test_table_test.testEqual( "names (mkX 3) = [\"x\"]"
        , m4921()
        , n160
        , m4941() )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4605):\n{e!s}")
    return(n177)

def m4618():
    try:
        n178 = default_table_test_table_test.printMsg("sliceRows", m4605())
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4618):\n{e!s}")
    return(n178)

def m4894(s137):
    try:
        n179 = default_table_test_table_test.testEqual( "sliceRows 5 9 t == empty (start past end)"
        , m4808(s137)
        , 0
        , m4618() )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4894):\n{e!s}")
    return(n179)

def m4888(s137):
    try:
        n180 = default_table_test_table_test.testEqual( "sliceRows 3 4 t == empty (start at end)"
        , m4797(s137)
        , 0
        , m4894(s137) )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4888):\n{e!s}")
    return(n180)

def m4882(s137, s133):
    try:
        n181 = default_table_test_table_test.testEqual( "sliceRows 2 3 t == [2] (last row)"
        , m4783(s137)
        , m4788(s133)
        , m4888(s137) )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4882):\n{e!s}")
    return(n181)

def m4876(s137, s135, s133):
    try:
        n182 = default_table_test_table_test.testEqual( "sliceRows 1 4 t == [1, 2] (end past nrow, clamped)"
        , m4769(s137)
        , m4774(s135)
        , m4882(s137, s133) )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4876):\n{e!s}")
    return(n182)

def m4870(s137, s135, s133):
    try:
        n183 = default_table_test_table_test.testEqual( "sliceRows 1 3 t == [1, 2]"
        , m4755(s137)
        , m4760(s135)
        , m4876(s137, s135, s133) )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4870):\n{e!s}")
    return(n183)

def m4864(s137, s135, s133, s132):
    try:
        n184 = default_table_test_table_test.testEqual( "sliceRows 1 2 t == [1] (single row)"
        , m4741(s137)
        , m4746(s132)
        , m4870(s137, s135, s133) )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4864):\n{e!s}")
    return(n184)

def m4858(s137, s135, s133, s132):
    try:
        n185 = default_table_test_table_test.testEqual( "sliceRows 1 1 t == empty (end == start)"
        , m4728(s137)
        , 0
        , m4864(s137, s135, s133, s132) )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4858):\n{e!s}")
    return(n185)

def m4852(s137, s135, s133, s132):
    try:
        n186 = default_table_test_table_test.testEqual( "sliceRows 1 0 t == empty (end < start, no reverse)"
        , m4715(s137)
        , 0
        , m4858(s137, s135, s133, s132) )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4852):\n{e!s}")
    return(n186)

def m4846(s137, s136, s135, s133, s132):
    try:
        n187 = default_table_test_table_test.testEqual( "sliceRows 0 3 t == full table"
        , m4701(s137)
        , m4706(s136)
        , m4852(s137, s135, s133, s132) )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4846):\n{e!s}")
    return(n187)

def m4840(s137, s136, s135, s134, s133, s132):
    try:
        n188 = default_table_test_table_test.testEqual( "sliceRows 0 2 t == [0, 1]"
        , m4687(s137)
        , m4692(s134)
        , m4846(s137, s136, s135, s133, s132) )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4840):\n{e!s}")
    return(n188)

def m4834(s137, s136, s135, s134, s133, s132, s131):
    try:
        n189 = default_table_test_table_test.testEqual( "sliceRows 0 1 t == [0]"
        , m4673(s137)
        , m4678(s131)
        , m4840(s137, s136, s135, s134, s133, s132) )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4834):\n{e!s}")
    return(n189)

def m4828(s137, s136, s135, s134, s133, s132, s131):
    try:
        n140 = ["x"]
        n190 = default_table_test_table_test.testEqual( "sliceRows 0 0 t preserves schema"
        , m4659(s137)
        , n140
        , m4834(s137, s136, s135, s134, s133, s132, s131) )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4828):\n{e!s}")
    return(n190)

def m4451(s137, s136, s135, s134, s133, s132, s131):
    try:
        n191 = default_table_test_table_test.testEqual( "sliceRows 0 0 t == empty"
        , m4646(s137)
        , 0
        , m4828(s137, s136, s135, s134, s133, s132, s131) )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4451):\n{e!s}")
    return(n191)

def m4464():
    try:
        s131 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-r")
        , 4622
        , [] )
        s132 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-r")
        , 4624
        , [] )
        s133 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-r")
        , 4626
        , [] )
        s134 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-r")
        , 4628
        , [] )
        s135 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-r")
        , 4631
        , [] )
        s136 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-r")
        , 4634
        , [] )
        s137 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-r")
        , 4638
        , [] )
        n192 = m4451(s137, s136, s135, s134, s133, s132, s131)
        n193 = default_table_test_table_test.printMsg("filterRows", n192)
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4464):\n{e!s}")
    return(n193)

def m4581(s25):
    try:
        n130 = ["x"]
        n194 = default_table_test_table_test.testEqual( "filterRows allFalse preserves schema"
        , m4529(s25)
        , n130
        , m4464() )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4581):\n{e!s}")
    return(n194)

def m4566(s27, s26, s25):
    try:
        n195 = default_table_test_table_test.testEqual( "filterRows mixed (mkX 3) keeps positions 0, 2"
        , m4516(s26)
        , m4522(s27)
        , m4581(s25) )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4566):\n{e!s}")
    return(n195)

def m4542(s27, s26, s25):
    try:
        n196 = default_table_test_table_test.testEqual( "filterRows allFalse (mkX 3) nrow = 0"
        , m4502(s25)
        , 0
        , m4566(s27, s26, s25) )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4542):\n{e!s}")
    return(n196)

def m4281(s27, s26, s25, s24):
    try:
        n197 = default_table_test_table_test.testEqual( "filterRows allTrue (mkX 3) == mkX 3"
        , m4488(s24)
        , m4494()
        , m4542(s27, s26, s25) )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4281):\n{e!s}")
    return(n197)

def m4294():
    try:
        n120 = [True, True, True]
        s24 = morloc.put_value(n120, mlc_schema_table[11])
        n121 = [False, False, False]
        s25 = morloc.put_value(n121, mlc_schema_table[11])
        n122 = [True, False, True]
        s26 = morloc.put_value(n122, mlc_schema_table[11])
        n123 = [0, 2]
        s27 = morloc.put_value(n123, mlc_schema_table[12])
        n198 = m4281(s27, s26, s25, s24)
        n199 = default_table_test_table_test.printMsg("pickRows", n198)
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4294):\n{e!s}")
    return(n199)

def m4427(s23, s20):
    try:
        n200 = default_table_test_table_test.testEqual( "pickRows [1] (mkX 3) picks the middle row"
        , m4375(s20)
        , m4381(s23)
        , m4294() )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4427):\n{e!s}")
    return(n200)

def m4412(s23, s22, s20, s19):
    try:
        n201 = default_table_test_table_test.testEqual( "pickRows duped (mkX 3) duplicates rows"
        , m4362(s19)
        , m4368(s22)
        , m4427(s23, s20) )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4412):\n{e!s}")
    return(n201)

def m4388(s23, s22, s21, s20, s19, s18):
    try:
        n202 = default_table_test_table_test.testEqual( "pickRows reversed (mkX 3) reverses"
        , m4347(s18)
        , m4353(s21)
        , m4412(s23, s22, s20, s19) )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4388):\n{e!s}")
    return(n202)

def m4171(s23, s22, s21, s20, s19, s18, s17):
    try:
        n203 = default_table_test_table_test.testEqual( "pickRows identity (mkX 3) == mkX 3"
        , m4333(s17)
        , m4339()
        , m4388(s23, s22, s21, s20, s19, s18) )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4171):\n{e!s}")
    return(n203)

def m4184():
    try:
        n105 = [0, 1, 2]
        s17 = morloc.put_value(n105, mlc_schema_table[4])
        n106 = [2, 1, 0]
        s18 = morloc.put_value(n106, mlc_schema_table[4])
        n107 = [0, 0, 1, 1, 2, 2]
        s19 = morloc.put_value(n107, mlc_schema_table[9])
        n108 = [1]
        s20 = morloc.put_value(n108, mlc_schema_table[10])
        n109 = [2, 1, 0]
        s21 = morloc.put_value(n109, mlc_schema_table[4])
        n110 = [0, 0, 1, 1, 2, 2]
        s22 = morloc.put_value(n110, mlc_schema_table[9])
        n111 = [1]
        s23 = morloc.put_value(n111, mlc_schema_table[10])
        n204 = m4171(s23, s22, s21, s20, s19, s18, s17)
        n205 = default_table_test_table_test.printMsg("distinctRows", n204)
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4184):\n{e!s}")
    return(n205)

def m4275(n15, s96):
    try:
        n206 = default_table_test_table_test.testEqual( "distinctRows of [1,2,1,3,2] == [1,2,3]"
        , m4244(s96)
        , n15
        , m4184() )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4275):\n{e!s}")
    return(n206)

def m4251(s99, n15, s96, s94):
    try:
        n207 = default_table_test_table_test.testEqual( "distinctRows of all-dup [7,7,7] == [7]"
        , m4234(s94)
        , m4237(s99)
        , m4275(n15, s96) )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4251):\n{e!s}")
    return(n207)

def m4010(s99, n15, s96, s94):
    try:
        n208 = default_table_test_table_test.testEqual( "distinctRows of all-distinct (mkX 3) == mkX 3"
        , m4221()
        , m4226()
        , m4251(s99, n15, s96, s94) )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4010):\n{e!s}")
    return(n208)

def m4023():
    try:
        s93 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-r")
        , 4188
        , [] )
        s94 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-r")
        , 4192
        , [s93] )
        s95 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-r")
        , 4196
        , [] )
        s96 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-r")
        , 4202
        , [s95] )
        s97 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-r")
        , 4206
        , [] )
        s98 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-r")
        , 4210
        , [s97] )
        n15 = morloc.get_value(s98, mlc_schema_table[0])
        s99 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-r")
        , 4214
        , [] )
        n209 = m4010(s99, n15, s96, s94)
        n210 = default_table_test_table_test.printMsg("sortRows", n209)
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4023):\n{e!s}")
    return(n210)

def m4138():
    try:
        n211 = default_table_test_table_test.testEqual( "sortRows [] (mkX 3) is a no-op"
        , m4087()
        , m4093()
        , m4023() )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4138):\n{e!s}")
    return(n211)

def m4123(s9):
    try:
        n212 = default_table_test_table_test.testEqual( "sortRows [(\"x\", False)] (mkX 3) reverses"
        , m4071()
        , m4080(s9)
        , m4138() )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4123):\n{e!s}")
    return(n212)

def m4099(s9):
    try:
        n213 = default_table_test_table_test.testEqual( "sortRows [(\"x\", True)] of [2, 1, 0] == mkX 3"
        , m4053(s9)
        , m4063()
        , m4123(s9) )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4099):\n{e!s}")
    return(n213)

def m3880(s9):
    try:
        n214 = default_table_test_table_test.testEqual( "sortRows [(\"x\", True)] (mkX 3) == mkX 3 (already asc)"
        , m4036()
        , m4045()
        , m4099(s9) )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3880):\n{e!s}")
    return(n214)

def m3893():
    try:
        n84 = [2, 1, 0]
        s9 = morloc.put_value(n84, mlc_schema_table[4])
        n215 = m3880(s9)
        n216 = default_table_test_table_test.printMsg("getCol", n215)
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3893):\n{e!s}")
    return(n216)

def m3948():
    try:
        n83 = [0.0, 0.5, 1.0]
        n217 = default_table_test_table_test.testEqual( "getCol \"z\" mkXYZ3 == zs3"
        , m3921()
        , n83
        , m3893() )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3948):\n{e!s}")
    return(n217)

def m3929():
    try:
        n81 = ["0", "1", "2"]
        n218 = default_table_test_table_test.testEqual( "getCol \"y\" mkXYZ3 == ys3"
        , m3913()
        , n81
        , m3948() )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3929):\n{e!s}")
    return(n218)

def m3683():
    try:
        n79 = [0, 1, 2]
        n219 = default_table_test_table_test.testEqual( "getCol \"x\" (mkX 3) == xs3"
        , m3901()
        , n79
        , m3929() )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3683):\n{e!s}")
    return(n219)

def m3696():
    try:
        n220 = default_table_test_table_test.printMsg("setCol", m3683())
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3696):\n{e!s}")
    return(n220)

def m3829(n8, n7):
    try:
        n221 = default_table_test_table_test.testEqual( "setCol \"z\" newZs mkXYZ3 replaces the existing z column"
        , n7
        , n8
        , m3696() )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3829):\n{e!s}")
    return(n221)

def m3793(n8, n7):
    try:
        n222 = default_table_test_table_test.testEqual( "setCol \"z\" zs (mkXY 3) == mkXYZ3 (append new column)"
        , m3736()
        , m3743()
        , m3829(n8, n7) )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3793):\n{e!s}")
    return(n222)

def m3520(n8, n7):
    try:
        n223 = default_table_test_table_test.testEqual( "setCol \"y\" ys (mkX 3) == mkXY 3 (append new column)"
        , m3721()
        , m3728()
        , m3793(n8, n7) )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3520):\n{e!s}")
    return(n223)

def m3533():
    try:
        s71 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-r")
        , 3700
        , [] )
        s72 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-r")
        , 3704
        , [s71] )
        n7 = morloc.get_value(s72, mlc_schema_table[6])
        s73 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-r")
        , 3709
        , [s71] )
        n8 = morloc.get_value(s73, mlc_schema_table[6])
        n224 = m3520(n8, n7)
        n225 = default_table_test_table_test.printMsg("dropCols", n224)
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3533):\n{e!s}")
    return(n225)

def m3626():
    try:
        n226 = default_table_test_table_test.testEqual( "dropCols [\"absent\"] (mkX 3) == mkX 3 (no-op)"
        , m3566()
        , m3573()
        , m3533() )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3626):\n{e!s}")
    return(n226)

def m3579():
    try:
        n227 = default_table_test_table_test.testEqual( "dropCols [\"x\", \"z\"] mkXYZ3 == mkY 3"
        , m3554()
        , m3560()
        , m3626() )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3579):\n{e!s}")
    return(n227)

def m3354():
    try:
        n228 = default_table_test_table_test.testEqual( "dropCols [\"z\"] mkXYZ3 == mkXY 3"
        , m3541()
        , m3546()
        , m3579() )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3354):\n{e!s}")
    return(n228)

def m3367():
    try:
        n229 = default_table_test_table_test.printMsg("selectCols", m3354())
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3367):\n{e!s}")
    return(n229)

def m3447():
    try:
        n230 = default_table_test_table_test.testEqual( "selectCols [\"x\", \"y\"] mkXYZ3 == mkXY 3"
        , m3403()
        , m3409()
        , m3367() )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3447):\n{e!s}")
    return(n230)

def m3415():
    try:
        n231 = default_table_test_table_test.testEqual( "selectCols [\"y\"] (mkXY 3) == mkY 3"
        , m3390()
        , m3397()
        , m3447() )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3415):\n{e!s}")
    return(n231)

def m3247():
    try:
        n232 = default_table_test_table_test.testEqual( "selectCols [\"x\"] (mkXY 3) == mkX 3"
        , m3375()
        , m3382()
        , m3415() )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3247):\n{e!s}")
    return(n232)

def m3260():
    try:
        n233 = default_table_test_table_test.printMsg("selectColsDyn", m3247())
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3260):\n{e!s}")
    return(n233)

def m3291():
    try:
        n234 = default_table_test_table_test.testEqual( "selectColsDyn [\"y\", \"z\"] mkXYZ3 keeps two columns"
        , m3279()
        , 2
        , m3260() )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3291):\n{e!s}")
    return(n234)

def m3166():
    try:
        n235 = default_table_test_table_test.testEqual( "selectColsDyn [\"x\"] mkXYZ3 == mkX 3"
        , m3268()
        , m3273()
        , m3291() )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3166):\n{e!s}")
    return(n235)

def m3179():
    try:
        n236 = default_table_test_table_test.printMsg("renameCol", m3166())
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3179):\n{e!s}")
    return(n236)

def m3215():
    try:
        n55 = [0, 1, 2]
        n237 = default_table_test_table_test.testEqual( "rename then getCol round-trip"
        , m3201()
        , n55
        , m3179() )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3215):\n{e!s}")
    return(n237)

def m2968():
    try:
        n238 = default_table_test_table_test.testEqual( "renameCol \"x\" \"a\" (mkX 3) renames"
        , m3187()
        , m3194()
        , m3215() )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m2968):\n{e!s}")
    return(n238)

def m2981():
    try:
        n239 = default_table_test_table_test.printMsg("rbind / cbind", m2968())
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m2981):\n{e!s}")
    return(n239)

def m3107():
    try:
        n240 = default_table_test_table_test.testEqual( "cbind (mkX 3) (mkY 3) == mkXY 3"
        , m3045()
        , m3053()
        , m2981() )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3107):\n{e!s}")
    return(n240)

def m3083():
    try:
        n241 = default_table_test_table_test.testEqual( "cbind (mkX 3) (mkY 3) ncol = 2"
        , m3031()
        , 2
        , m3107() )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3083):\n{e!s}")
    return(n241)

def m3059(n5):
    try:
        n242 = default_table_test_table_test.testEqual( "rbind (mkX 3) (mkX 3) == doubled column"
        , m3017()
        , n5
        , m3083() )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3059):\n{e!s}")
    return(n242)

def m2858(n5):
    try:
        n243 = default_table_test_table_test.testEqual( "rbind (mkX 3) (mkX 3) nrow = 6"
        , m3001()
        , 6
        , m3059(n5) )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m2858):\n{e!s}")
    return(n243)

def m2871():
    try:
        s45 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-r")
        , 2985
        , [] )
        s46 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-r")
        , 2992
        , [s45] )
        n5 = morloc.get_value(s46, mlc_schema_table[0])
        n244 = m2858(n5)
        n245 = default_table_test_table_test.printMsg( "Composition / derived"
        , n244 )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m2871):\n{e!s}")
    return(n245)

def m2962(s38, s37):
    try:
        n246 = default_table_test_table_test.testEqual( "reverseRows t == pickRows (reverse (range 0 (nrow t - 1))) t"
        , m2926(s38)
        , m2940(s37)
        , m2871() )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m2962):\n{e!s}")
    return(n246)

def m2956(s38, s37, s36):
    try:
        n247 = default_table_test_table_test.testEqual( "tail k t == sliceRows (nrow t - k) (nrow t) t"
        , m2907(s38)
        , m2919(s36)
        , m2962(s38, s37) )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m2956):\n{e!s}")
    return(n247)

def m2852(s38, s37, s36, s35):
    try:
        n248 = default_table_test_table_test.testEqual( "head k t == sliceRows 0 k t"
        , m2893(s38)
        , m2898(s35)
        , m2956(s38, s37, s36) )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m2852):\n{e!s}")
    return(n248)

def m2839():
    try:
        s35 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-r")
        , 2875
        , [] )
        s36 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-r")
        , 2878
        , [] )
        s37 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-r")
        , 2881
        , [] )
        s38 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-r")
        , 2885
        , [] )
        n249 = m2852(s38, s37, s36, s35)
        n250 = default_table_test_table_test.printResult(n249)
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m2839):\n{e!s}")
    return(morloc.put_value(n250, mlc_schema_table[14]))
# AUTO include manifolds end


# AUTO include dispatch start
dispatch = {
    2839: m2839,
}
remote_dispatch = {
}
# AUTO include dispatch end

def run_job(client_fd: int) -> None:
    try:
        # Free SHM from previous dispatch result (consumed by caller)
        morloc.flush_shm_tracker()
        client_data = morloc.stream_from_client(client_fd)

        if(morloc.is_local_call(client_data)):
            (mid, args) = morloc.read_morloc_call_packet(client_data)

            try:
                result = dispatch[mid](*args)
            except Exception as e:
                result = morloc.make_fail_packet(str(e))

        elif(morloc.is_remote_call(client_data)):
            (mid, args) = morloc.read_morloc_call_packet(client_data)

            try:
                result = remote_dispatch[mid](*args)
            except Exception as e:
                result = morloc.make_fail_packet(str(e))

        elif(morloc.is_ping(client_data)):
            result = morloc.pong(client_data)

        else:
            raise ValueError("Expected a ping or call type packet")

        # Flush stdout BEFORE sending the result back. The nexus prints its
        # own output (the return value) right after receiving this response.
        # Both processes share the same stdout fd, so if we flush after sending,
        # the nexus can print first, causing out-of-order output.
        sys.stdout.flush()

        morloc.send_packet_to_foreign_server(client_fd, result)

    except Exception as e:
        # Try to send a fail packet back to the caller before giving up.
        # This may fail (e.g., broken pipe from a timed-out ping), which is OK.
        try:
            result = morloc.make_fail_packet(str(e))
            morloc.send_packet_to_foreign_server(client_fd, result)
        except Exception:
            pass
        print(f"job failed: {e!s}", file=sys.stderr)
    finally:
        # Safety-net flush for any output from error handling paths
        sys.stdout.flush()
        # close child copy
        morloc.close_socket(client_fd)


def _send_fd(sock, fd):
    """Send a file descriptor over a Unix domain socket."""
    sock.sendmsg([b'\x00'],
                 [(_socket.SOL_SOCKET, _socket.SCM_RIGHTS,
                   array.array('i', [fd]))])

def _recv_fd(sock):
    """Receive a file descriptor from a Unix domain socket."""
    msg, ancdata, flags, addr = sock.recvmsg(1, _socket.CMSG_SPACE(4))
    if not msg and not ancdata:
        raise EOFError("Connection closed")
    for cmsg_level, cmsg_type, cmsg_data in ancdata:
        if (cmsg_level == _socket.SOL_SOCKET and
                cmsg_type == _socket.SCM_RIGHTS):
            a = array.array('i')
            a.frombytes(cmsg_data[:4])
            return a[0]
    raise RuntimeError("No fd received in ancillary data")


WORKER_IDLE_TIMEOUT = 5.0  # seconds before an idle worker exits

def worker_process(job_fd, tmpdir, shm_basename, shutdown_flag, busy_count, total_workers, wakeup_w):
    # Reset signal handlers inherited from main. If user code inside run_job
    # calls multiprocessing.Pool (or anything else that forks and later
    # SIGTERMs its own children), those grandchildren would otherwise inherit
    # main's signal_handler and flip the shared shutdown_flag, causing main
    # to SIGKILL this worker mid-response. See the multiprocessing-py-1 bug.
    signal.signal(signal.SIGTERM, signal.SIG_DFL)
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    morloc.set_fallback_dir(tmpdir)
    morloc.shinit(shm_basename, 0, 0xffff)
    _init_worker_tracking(busy_count, total_workers, wakeup_w)
    sock = _socket.fromfd(job_fd, _socket.AF_UNIX, _socket.SOCK_STREAM)
    os.close(job_fd)  # sock owns a dup'd copy
    last_activity = time.monotonic()
    try:
        while not shutdown_flag.value:
            rlist, _, _ = select.select([sock.fileno()], [], [], 0.01)
            if shutdown_flag.value:
                break
            if rlist:
                try:
                    client_fd = _recv_fd(sock)
                    run_job(client_fd)
                    last_activity = time.monotonic()
                except (EOFError, OSError):
                    break
            elif total_workers.value > 1 and time.monotonic() - last_activity > WORKER_IDLE_TIMEOUT:
                break
    except BaseException as e:
        # Catch-all for errors that escape run_job's own exception handling:
        # MemoryError, KeyboardInterrupt, SystemExit, or bugs in the worker
        # loop itself. Without this, the worker dies silently and the nexus
        # only sees "failed to read response header" with no indication of
        # what went wrong in the pool.
        #
        # Race condition: the nexus detects the broken socket and may start
        # its clean_exit tear-down (SIGTERM -> SIGKILL) while this print is
        # still buffered. We flush immediately to maximize the chance the
        # message reaches the terminal before we are killed. stderr is
        # line-buffered (set in __main__), but the flush is a safety net for
        # edge cases (redirected stderr, forked-process buffer state).
        import traceback
        print(f"morloc pool worker fatal error: {e!s}", file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
        sys.stderr.flush()
    finally:
        sock.close()


def signal_handler(sig, frame):
    global daemon
    # Ignore further SIGTERM/SIGINT during cleanup. Python processes pending
    # signals between bytecodes, including while another signal handler is
    # running, so a second SIGTERM arriving mid-cleanup would otherwise
    # re-enter this handler and double-free the daemon pointer.
    try:
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        signal.signal(signal.SIGINT, signal.SIG_IGN)
    except Exception:
        pass
    shutdown_flag.value = True
    if _shutdown_wakeup_fd >= 0:
        try:
            os.write(_shutdown_wakeup_fd, b'!')
        except OSError:
            pass
    # Capture the daemon pointer into a local and clear the global BEFORE
    # invoking close_daemon. If a pending signal still slips through and
    # re-enters this handler, it will see daemon=None and skip the free.
    d = daemon
    daemon = None
    if d is not None:
        morloc.close_daemon(d)


def client_listener(job_fd, socket_path, tmpdir, shm_basename, shutdown_flag):
    global daemon
    daemon = morloc.start_daemon(socket_path, tmpdir, shm_basename, 0xffff)
    sock = _socket.fromfd(job_fd, _socket.AF_UNIX, _socket.SOCK_STREAM)
    os.close(job_fd)  # sock owns a dup'd copy

    while not shutdown_flag.value:
        try:
            client_fd = morloc.wait_for_client(daemon)
        except Exception as e:
            print(f"In python daemon, failed to connect to client: {e!s}", file=sys.stderr)
            continue

        if client_fd > 0:
            try:
                _send_fd(sock, client_fd)
            except Exception as e:
                print(f"In python daemon, failed to start worker: {e!s}", file=sys.stderr)
            finally:
                morloc.close_socket(client_fd)
    sock.close()



if __name__ == "__main__":
    # Line-buffer stderr so diagnostic output is not lost when pool is killed.
    # stdout is left fully buffered for performance (genome-scale piping) and
    # flushed explicitly after each job and during shutdown.
    sys.stderr.reconfigure(line_buffering=True)

    # Request SIGTERM when the parent process (nexus) dies.
    # Without this, SIGKILL on the nexus leaves pool processes orphaned
    # and their SHM segments leak in /dev/shm.
    try:
        import ctypes
        _PR_SET_PDEATHSIG = 1
        ctypes.CDLL("libc.so.6", use_errno=True).prctl(_PR_SET_PDEATHSIG, signal.SIGTERM)
    except Exception:
        pass  # non-Linux: skip (macOS uses kqueue for this)

    shutdown_flag = Value('b', False)  # Shared flag

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Health check: confirm imports loaded and print version
    if len(sys.argv) > 1 and sys.argv[1] == "--health":
        sys.stdout.write('{"status":"ok","version":"0.82.0"}\n')
        sys.exit(0)

    # Process arguments passed from the nexus
    try:
        socket_path = sys.argv[1]
        tmpdir = sys.argv[2]
        shm_basename = sys.argv[3]
    except IndexError:
        print("Usage: script.py <socket_path> <tmpdir> <shm_basename>")
        sys.exit(1)

    global_state["tmpdir"] = tmpdir

    # Shared job queue: listener writes fds to write_sock, workers read from read_sock.
    # Only idle workers (blocked in recvmsg) pick up jobs, preventing the round-robin
    # deadlock where a callback gets dispatched to a busy worker.
    read_sock, write_sock = _socket.socketpair(_socket.AF_UNIX, _socket.SOCK_STREAM)

    num_workers = 1
    workers = []

    # Shared counters for dynamic worker spawning.
    # Workers increment busy_count before foreign_call and decrement after.
    # When all workers are busy, main process spawns a new one.
    busy_count = RawValue(ctypes.c_int, 0)
    total_workers = RawValue(ctypes.c_int, num_workers)
    wakeup_r, wakeup_w = os.pipe()
    os.set_blocking(wakeup_r, False)
    _shutdown_wakeup_fd = wakeup_w

    # Keep a dup of the read end so we can spawn new workers later
    spare_read_fd = os.dup(read_sock.fileno())

    for i in range(num_workers):
        worker = Process(target=worker_process,
                         args=(read_sock.fileno(), tmpdir, shm_basename, shutdown_flag,
                               busy_count, total_workers, wakeup_w))
        worker.start()
        workers.append(worker)
    read_sock.close()  # main/listener don't need the read end (spare_read_fd kept)

    # Start client listener process
    listener_process = Process(
        target=client_listener,
        args=(write_sock.fileno(), socket_path, tmpdir, shm_basename, shutdown_flag)
    )
    listener_process.start()
    write_sock.close()  # main doesn't need the write end

    # Main loop: monitor wake-up pipe, spawn new workers when all are busy,
    # and reap idle workers that have exited.
    while not shutdown_flag.value:
        rlist, _, _ = select.select([wakeup_r], [], [], 0.01)
        if rlist:
            try:
                os.read(wakeup_r, 4096)  # drain pipe
            except OSError:
                pass

        # Reap dead workers (idle timeout or error exit)
        alive = []
        for w in workers:
            if w.is_alive():
                alive.append(w)
            else:
                w.join(timeout=0)
                w.close()
        workers = alive
        total_workers.value = max(1, len(workers))

        # Spawn a new worker if all are busy (or all have exited)
        if len(workers) == 0 or busy_count.value >= total_workers.value:
            w = Process(target=worker_process,
                        args=(spare_read_fd, tmpdir, shm_basename, shutdown_flag,
                              busy_count, total_workers, wakeup_w))
            w.start()
            workers.append(w)
            total_workers.value = len(workers)

    # Shutdown sequence
    os.close(wakeup_r)
    os.close(wakeup_w)
    os.close(spare_read_fd)

    # 1. Stop listener first
    listener_process.terminate()
    listener_process.join(timeout=0.001)
    listener_process.kill()
    listener_process.join()  # Final blocking reap
    listener_process.close()

    # 2. Terminate workers with escalating force
    for p in workers:
        if p.is_alive():
            p.kill()
        p.join()  # Final blocking reap
        p.close()

    sys.exit(0)
