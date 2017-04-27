#!/usr/bin/env python

"""Get today's date"""
from datetime import datetime

def main():
    """Main function to get today's date"""
    now = datetime.now()
    utcnow = datetime.utcnow()
    fmt = '%Y-%m-%d %H:%M:%S.0'
    out = {
        'NOW': now.strftime(fmt),
        'UTCNOW': utcnow.strftime(fmt),
        'DATE': now.strftime('%Y-%m-%d')
    }

    print '\n'.join(['%s=%s' % (k, v) for k, v in out.items()])

if __name__ == '__main__':
    main()
