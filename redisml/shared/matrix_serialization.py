import numpy as np

def dumps(matrix):
    flat = []
    flat.append(str(matrix.shape[0]))
    flat.append(',')
    flat.append(str(matrix.shape[1]))
    
    zero_count = 0
    for e in matrix.flat:
        if e == 0:
            zero_count += 1
        else:
            if zero_count != 0:
                if zero_count < 3:
                    for i in range(0,zero_count):
                        flat.append(';0')
                else:
                    flat.append(';')
                    flat.append('x')
                    flat.append(str(zero_count))
                    
                zero_count = 0

            flat.append(';')
            flat.append(str(e))
    return ''.join(flat)
    
def loads(s):
    elements = s.split(';')
    shape = elements[0].split(',')
    width = int(shape[0])
    height = int(shape[1])
    flat = []
    for e in elements[1:]:
        if e[0] == 'x':
            num = int(e[1:])
            for i in range(0,num):
                flat.append(0)
        else:
            flat.append(float(e))
    return np.reshape(flat, (width, height))