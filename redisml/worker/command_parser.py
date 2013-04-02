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