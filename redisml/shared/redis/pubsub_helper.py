import threading
from lib.Event import Event

class ChannelListener():
    
    def __init__(self, channels, red):
        self.channels = channels
        self.red = red 
        self.stop = False
        self.onMessage = Event()

    def _listen(self):
        self.pubsub = self.red.pubsub()
        
        for c in self.channels:
            self.pubsub.subscribe(c) 
        
        while not self.stop:
            for msg in self.pubsub.listen():
                if msg['type'] == 'message':
                    self.onMessage(msg['channel'], msg['data'])
        
        for c in self.channels:
            self.pubsub.unsubscribe(c)
    
    def start(self):
        self.stop = False
        self.thread = threading.Thread(target=self._listen)
        self.thread.deamon = True
        self.thread.start()
        
    def stop(self):
        if self.thread != None:
            self.stop = True
            self.thread.join(5)
            return not self.thread.isAlive()
        return True