import os

path = '/home/jardel/fsteste/file_one'
mnt = '/home/jardel/fsteste'

with open(path, 'w+') as fh1:
    fh1.write('foo')
    fh1.flush()
    with open(path, 'a') as fh2:
        os.unlink(path)
        assert 'file_one' not in os.listdir(mnt)
        fh2.write('bar')
    os.close(os.dup(fh1.fileno()))
    fh1.seek(0)
    assert fh1.read() == 'foobar'