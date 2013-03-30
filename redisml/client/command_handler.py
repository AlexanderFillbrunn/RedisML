import command_parser
import client_settings

def execute(r, cmd_str):
    cmds = cmd_str.split('\n')
    for cmd in cmds:
        # get command name and parameters
        c = command_parser.parse_command(cmd)
        
        if not client_settings.COMMAND_MAPPING.has_key(c[0]):
            raise Exception('Command ' + c[0] + ' is not registered')
        
        func = client_settings.COMMAND_MAPPING[c[0]]
        
        func_parts = func.split('.')
        mod = __import__(func[:func.rindex('.')])
        
        for i in range(1, len(func_parts)-1):
            mod = getattr(mod, func_parts[i])
        
        f = getattr(mod, func_parts[len(func_parts)-1])
        f(r, c[1:])