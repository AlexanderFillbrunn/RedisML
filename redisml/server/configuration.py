import json
import exceptions

def load_config(cfg_file):
    with open(cfg_file) as file:
        cfg = json.loads(file.read())
        return cfg
                    
def clean_config(cfg, host='localhost', port=6379, db=0, block_size=1024):
    # Redis stuff
    if not cfg.has_key('server_name'):
        raise exceptions.ConfigurationExeption('The configuration does not have a server name')
    if not cfg.has_key('redis'):
        cfg['redis'] = {}
    redis_cfg = cfg['redis']
    if not redis_cfg.has_key('host'):
        redis_cfg['host'] = host
    if not redis_cfg.has_key('port'):
        redis_cfg['port'] = port
    if not redis_cfg.has_key('db'):
        redis_cfg['db'] = db
    # Matrix stuff
    if not cfg.has_key('matrix'):
        cfg['matrix'] = {}
    matrix_cfg = cfg['matrix']  
    if not matrix_cfg.has_key('block_size'):
        matrix_cfg['block_size'] = block_size
