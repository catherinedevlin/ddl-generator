import argparse
import logging
try:
    from ddlgenerator.ddlgenerator import Table, dialect_names
except ImportError:
    from ddlgenerator import Table, dialect_names  # TODO: can py2/3 split this
# If anyone can explain these import differences to me, I will buy you a cookie.

def read_args():
    parser = argparse.ArgumentParser(description='Generate DDL based on data')
    parser.add_argument('dialect', help='SQL dialect to output', type=str.lower)
    parser.add_argument('datafile', help='Path to file storing data (accepts .yaml, .json)')
    parser.add_argument('-k', '--key', help='Field to use as primary key', type=str.lower)
    parser.add_argument('-i', '--inserts', action='store_true', help='also generate INSERT statements')
    parser.add_argument('-t', '--text', action='store_true', help='Use variable-length TEXT columns instead of VARCHAR')
    parser.add_argument('-u', '--uniques', action='store_true', help='Include UNIQUE constraints where data is unique')
    parser.add_argument('-l', '--log', type=str.upper, help='log level (CRITICAL, FATAL, ERROR, DEBUG, INFO, WARN)', default='WARN')
    args = parser.parse_args()
    return args
    
def set_logging(args):
    try:
        loglevel = int(getattr(logging, args.log))
    except (AttributeError, TypeError) as e:
        raise NotImplementedError('log level "%s" not one of CRITICAL, FATAL, ERROR, DEBUG, INFO, WARN' % args.log)
    logging.getLogger().setLevel(loglevel)

def generate():
    args = read_args()
    set_logging(args)
    logging.info(str(args))
    if args.dialect in ('pg', 'pgsql', 'postgres'):
        args.dialect = 'postgresql'
    if args.dialect not in dialect_names:
        raise NotImplementedError('First arg must be one of: %s' % ", ".join(dialect_names))
    table = Table(args.datafile, varying_length_text=args.text, uniques=args.uniques, 
                  pk_name = args.key, loglevel=args.log)
    print(table.sql(args.dialect, args.inserts))
   