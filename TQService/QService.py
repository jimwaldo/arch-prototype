'''
Created on May 12, 2013

A prototype implementation of the v2 asynchronous task service. This prototype
implements a simple Queue, a Task object, and a TaskStatus object, along with a
proxy object that can be handed around to different processes (perhaps on different
machines) that can be used to share the TaskStatus object.

Communication in this prototype is via rpyc, which is a simple rpc system with a 
service registry. 
'''
import rpyc
import uuid
from collections import deque
from socket import gethostname

class XTask:
    '''
    A simple Task object, containing a uuid, a destination id, and some chunk
    of data. This is more to test things than to do anything useful
    '''
    def __init__(self, destId, data):
        self.id = str(uuid.uuid4())
        self.destId = destId
        self.data = data
        
class TQStatus(object):
    '''
    An enumeration of the states that a Task can be in, reflected in the
    TaskStatus object (and obtainable by using the TSProxy). The states
    should be taken as a set of flags that indicate the state as last updated;
    there may be some lag between the time that a Task actually changes
    state and the time at which the status is changed
    '''
    queued = 0
    taken = 1
    cancelled = 2
    complete = 3
    error = 4
       
class TaskStatus(object):
    def __init__(self, taskId, retDest):
        self.id = taskId
        self.retDest = retDest
        self.qStatus = TQStatus.queued;
        self.progress = 0.0
        
class TSProxy(TaskStatus):
    def __init__(self, taskId, retDest, host, port):
        super().__init__(taskId, retDest)
        self.host = host
        self.port = port
        self.connected = False
        
    def connectToServer(self):
        self.service = rpyc.connect(self.host, self.port)
        self.connected = True
        
    def __getattribute__(self, attrName):
        if (not self.connected):
            self.connectToServer()
        return self.service.getAttr(self.id, attrName)
    
    def __setattr__(self, attrName, newVal):
        if (not self.connected):
            self.connectToServer()
        self.__dict__[attrName] = self.service.setAttr(self.id, attrName, newVal)
        
class TestQueueService(rpyc.Service):
    '''
    A prototype of the v2 edX asynchronous task queue service
    
    This service allows putting tasks into a queue, removing those tasks from the queue,
    and tracking the status and progress of the task as it is worked on. The queue itself 
    if very simple, being a FIFO queue of Task objects. Associated with each Task is a 
    TaskStatus object, that is used to store information that is shared between the client,
    the worker, and the task itself. 
    
    The current implementation uses RPYC, a simple rpc mechanism for python clients and
    servers that are built to run within the same administrative domain.
    '''

    def on_connect(self):
        self.hostname = gethostname()
        self.socAddr = 19962
        self.queue = deque([])
        self.tStatus = {}
        print ('Test Queue Service has been started')
            
    
    def on_disconnect(self):
        pass
    
    def exposed_put(self, taskId, taskdest, taskdata):
        '''
        Put a task on the queue, returning a TSProxy that allow the client to track task progress
        
        
        '''
        task = XTask(taskId, taskdest, taskdata)
        retTS = TaskStatus(taskid, taskdest)
        self.queue.append(task)
        self.tStatus[task.id] = retTS
        retProxy = (task.id, task.destId, self.hostname, self.socAddr)
        return retProxy
    
    def exposed_get(self):
        retTask = self.queue.popleft()
        self.tStatus[retTask.id].qStatus = TQStatus.taken
        retTS = TSProxy(retTask.id, retTask.destId, self.hostname, self.socAddr)
        return (retTask.id, retTask.destId, retTask.data, retTS.id, retTask.destid, retTask.data)
    
    def exposed_getAttr(self, taskId, attrName):
        if (attrName == 'qStatus'):
            return self.tStatus[taskId].qStatus
        elif (attrName == 'progress'):
            return self.tStatus[taskId].progress
    
    def exposed_setAttr(self, taskId, attrName, newVal):
        if (attrName == 'qStatus'):
            self.tStatus[taskId].qStatus = newVal
        elif (attrName == 'progress'):
            self.tStatus[taskId].progress = newVal
        

if __name__ == '__main__':
    from rpyc.utils.server import ThreadedServer
    t = ThreadedServer(TestQueueService, port = 19962)
    t.start()
        