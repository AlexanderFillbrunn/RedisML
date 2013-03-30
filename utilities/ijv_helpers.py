def to_ijv(a):
    rows, cols = a.shape
    ijv = np.empty((a.size,), dtype=[('i', np.intp),
                                     ('j', np.intp),
                                     ('v', a.dtype)])
    ijv['i'] = np.repeat(np.arange(rows), cols)
    ijv['j'] = np.tile(np.arange(cols), rows)
    ijv['v'] = a.ravel()
    return ijv

def from_ijv(ijv):
    rows, cols = np.max(ijv['i']) + 1, np.max(ijv['j']) + 1
    a = np.empty((rows, cols), dtype=ijv['v'].dtype)
    a[ijv['i'], ijv['j']] = ijv['v']
    return a
    
def save_ijv(file_, a):
    ijv = to_ijv(a)
    np.savetxt(file_, ijv, delimiter=';', fmt=('%d', '%d', '%f'))

def read_ijv(file_):
    ijv = np.loadtxt(file_, delimiter=';',
                     dtype=[('i', np.intp),('j', np.intp),
                            ('v', np.float)])
    return from_ijv(ijv)