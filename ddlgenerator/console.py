import argparse
import logging
import sys
try:
    from ddlgenerator.ddlgenerator import Table
except ImportError:
    from ddlgenerator import Table
# If anyone can explain these import differences to me, I will buy you a cookie.

def read_args():
    parser = argparse.ArgumentParser(description='Generate DDL based on data')
    parser.add_argument('dialect', help='SQL dialect to output', type=str.lower)
    parser.add_argument('datafile', help='Path to file storing data (accepts .yaml, .json)')
    parser.add_argument('-i', '--inserts', action='store_true', help='also generate INSERT statements')
    parser.add_argument('-t', '--text', action='store_true', help='Use variable-length TEXT columns instead of VARCHAR')
    parser.add_argument('-u', '--uniques', action='store_true', help='Include UNIQUE constraints where data is unique')
    parser.add_argument('-l', '--log', type=str, help='log level (CRITICAL, FATAL, ERROR, DEBUG, INFO, WARN)', default='WARN')
    args = parser.parse_args()
    return args
    
def set_logging(args):
    try:
        loglevel = int(getattr(logging, args.log.upper()))
    except (AttributeError, TypeError) as e:
        raise NotImplementedError('log level "%s" not one of CRITICAL, FATAL, ERROR, DEBUG, INFO, WARN' % args.log)
    logging.getLogger().setLevel(args.log)

dialects = 'drizzle firebird mssql mysql oracle postgresql sqlite sybase'.split()

def generate():
    args = read_args()
    set_logging(args)
    logging.info(str(args))
    if args.dialect in ('pg', 'pgsql', 'postgres'):
        args.dialect = 'postgresql'
    if args.dialect not in dialects:
        raise NotImplementedError('First arg must be one of: %s' % ", ".join(dialects))
    table = Table(args.datafile, varying_length_text=args.text, uniques=args.uniques, loglevel=args.log)
    print(table.sql(args.dialect, args.inserts))
   