
# ======= Command building =======

def build_command(name, *params):
    #Simple method for building commands
    output = name 
    for p in params:
        output += ' ' + str(p)
    return output

def escape_expression(expr):
    if expr.find('#') != -1:
        raise Exception('Expressions must not contain comments')
    return expr.replace(' ', '#')

def unescape_expression(expr):
    return expr.replace('#', ' ')

class CommandBuilder:
    '''
    Builds commands for sending to clients
    '''
    def __init__(self, cmd_name):
        self.cmd_name = cmd_name
        self.params = []
        
    def add_param(self, param):
        ''' Adds a parameter to the command '''
        self.params.append(' ' + str(param))
    
    def getCmdString(self):
        ''' Returns the built command as a string '''
        return self.cmd_name + ' '.join(self.params)
    
    def join(self, cb):
        ''' Joins the command with another one and returns the string '''
        return self.getCmdString() + '\n' + cb.getCmdString()
    
    def __str__(self):
        return self.getCmdString()
        
# ======= Command parsing =======

def parse_command(cmd):
    idx = 0
    current = ''
    result = []
    escape = False
    in_string = False
    
    while idx < len(cmd):
        c = cmd[idx]
        if c == ' ':
            if not in_string and len(current) > 0:
                result.append(current)
                current = ''
            elif in_string:
                current += c
            escape = False
        elif c == '"':
            if escape:
                current += c
            elif in_string:
                in_string = False
            else:
                in_string = True
            escape = False
        elif c == '\\':
            if escape:
                current += c
            escape = not escape    
        else:
            current += c
            escape = False
        idx += 1
            
    result.append(current)
    
    return result
    
# ======= Command names =======

MATRIXMULT              = 'MMULT'
MATRIXADDITION          = 'MADD'
KMEANSDIST              = 'DIST'
KMEANSRECALC            = 'RECALC'
TRANSPOSE               = 'MTRANS'  
CELLWISEOP              = 'CW'
BINARYMATRIXOP          = 'MBIN'
MATRIXSCALAROP          = 'MS'
CREATE                  = 'MCREATE'
AGGREGATEOP             = 'MAGGR'
EQUALS                  = 'EQUAL'
COUNT                   = 'COUNT'
DELETE                  = 'DEL'
FAIL                    = 'FAIL'
TIMEOUT                 = 'TIMEOUT'
TRACE                   = 'MTRACE'