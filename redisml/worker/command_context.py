
class CommandContext:
    def __init__(self, redis_master, key_mngr, cmdArgs):
        self.redis_master = redis_master
        self.key_manager = key_mngr
        self.cmdArgs = cmdArgs