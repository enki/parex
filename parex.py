# parex - Parallel Execution for Python
# (c) 2010 by Paul Bohm http://paulbohm.com/
# MIT License.
#
# Execute multiple processes in parallel and then wait for all to finish.
# I use this for faster deployment with fabric, so commands don't block.
# For best results combine with SSH multiplexing (ControlMaster).
#
# Usage (Standalone):
#   x = TaskManager(cwd=".")
#   x.execute('ls') # execute commands
#   x.execute('ps auxw')
#   pid = x.execute('who') # returns pid
#   x.wait() # wait till all have finished
#   for y in x.data.values(): # keyed by pid
#      print y.getvalue() # y is StringIO
#
# Also integrates nicely with fabric. Instead of doing blocking
#   with cd("foo/bar"):
#     run('python bbqctl.py daemon fanoutserver')
#     run('python bbqctl.py daemon main')
#     run('python bbqctl.py daemon twitresolve')
#     run('python bbqctl.py daemon rssparse')
# on the fabric client. Just do
#   x = TaskManager("foo/bar")
#   x.execute('python bbqctl.py daemon fanoutserver')
#   x.execute('python bbqctl.py daemon main')
#   x.execute('python bbqctl.py daemon twitresolve')
#   x.execute('python bbqctl.py daemon rssparse')
#   x.wait()
# on the server. You can use fabric to make the server run your code.
#

import os
import select
import subprocess
from subprocess import PIPE, Popen
from cStringIO import StringIO

# Constants from the epoll module/tornado
_EPOLLIN = 0x001
_EPOLLPRI = 0x002
_EPOLLOUT = 0x004
_EPOLLERR = 0x008
_EPOLLHUP = 0x010
_EPOLLRDHUP = 0x2000
_EPOLLONESHOT = (1 << 30)
_EPOLLET = (1 << 31)

NONE = 0
READ = _EPOLLIN
WRITE = _EPOLLOUT
ERROR = _EPOLLERR | _EPOLLHUP | _EPOLLRDHUP

FDIN = READ
FDOUT = WRITE
FDERR = ERROR

if hasattr(select, "epoll"):
    # Python 2.6+ on Linux
    _poll = select.epoll
else:
    from tornado.ioloop import _poll

class TaskManager:
    def __init__(self, cwd='.'):
        self.cwd = cwd
        self.processes = {}
        self.processgbprot = {}
        self.fds = {}
        self.fdtype = {}
        self.data = {}
        
    def execute(self, cmd):
        fullcmd = 'cd ' + self.cwd + '; ' + cmd
        print 'Executing', fullcmd
        p = Popen(fullcmd, shell=True, #bufsize=bufsize,
                  stdin=PIPE, stdout=PIPE, stderr=PIPE, close_fds=True)
        self.processes[p.pid] = ( p.stdin.fileno(), p.stdout.fileno(), p.stderr.fileno() )
        self.processgbprot[p.pid] = ( p.stdin, p.stdout, p.stderr )
        
        self.fds[p.stdin.fileno()] = p.pid
        self.fdtype[p.stdin.fileno()] = FDIN
        
        self.fds[p.stdout.fileno()] = p.pid
        self.fdtype[p.stdout.fileno()] = FDOUT
        
        self.fds[p.stderr.fileno()] = p.pid
        self.fdtype[p.stderr.fileno()] = FDERR
        
        return p.pid
    
    def wait(self):        
        while self.processes:
            iop = _poll()
            for fd in [x for x in self.fds if self.fdtype[x] in (FDOUT,FDERR)]:
                iop.register(fd, READ)
            
            active = iop.poll(0.5)
            for fd, state in active:

                pid = self.fds.get(fd, None)                
                d = ''
                if state == READ:
                    d = os.read(fd, 4096)
                elif state in (_EPOLLHUP, _EPOLLRDHUP):
                    self.processgbprot[pid][self.processes[pid].index(fd)].close()
                
                if len(d) == 0:
                    del self.fds[fd]
                    remaining = [x for x in self.processes[pid] if self.fds.get(x, None)]
                    if len(remaining) == 1:
                        del self.processes[pid]
                        del self.processgbprot[pid]
                else:
                    res = self.data.setdefault(pid, StringIO())
                    res.write(d)

if __name__ == '__main__':
    x = TaskManager()
    x.execute('ls')
    x.execute('ps auxw')
    x.execute('who')
    x.wait()
    for y in x.data.values():
        print y.getvalue()