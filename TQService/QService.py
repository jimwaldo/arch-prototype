'''
Created on May 12, 2013

@author: waldo
'''
import rpyc
import uuid
from collections import deque

class XTask:
    def __init__(self, destId, data):
        self.id = uuid.uuid4()
        self.destId = destId
        self.data = data
        

class TQStatus(object):
    queued = 0
    taken = 1
    cancelled = False
    complete = False
    error = ''
    
    
    
class TaskStatus(object):
    def __init__(self, taskId, retDest):
        self.id = taskId
        self.retDest = retDest
        self.qStatus = TQStatus.queued;
        self.progress = 0.0
        
class TSProxy(TaskStatus):
    def __init__(self, tqs):
        self.queueSerice = tqs
        
    
class TestQueueService(rpyc.Service):
    '''
    classdocs
    '''

    def __init__(self):
        '''
        Constructor
        '''
        self.queue = deque([])
        self.tStatus = {}
        
    
    def on_connect(self):
        pass
    
    def on_disconnect(self):
        pass
    
    def exposed_put(self, task):
        retTS = TaskStatus(task.id, task.destId)
        self.queue.append(task)
        self.tStatus[task.id] = retTS
        return retTS
    
    def exposed_get(self):
        retTask = self.queue.popleft()
        retTS = TSProxy(self.tStatus[retTask.id], self)
        return (retTask, retTS)
    
    def exposed_get_status(self, taskId):
        pass
    
    def exposed_cancel_task(self, taskId):
        pass
    
    def exposed_update_progress(self, taskId, progress):
        pass
    
    def exposed_update_taskStatus(self, taskId, newStatus):
        pass
    
    
        