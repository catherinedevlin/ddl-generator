import argparse
import sys
if sys.version_info.major == 2:
    from ddlgenerator import Table
else:
    from ddlgenerator.ddlgenerator import Table
# If anyone can explain these import differences to me, I will buy you a cookie.

parser = argparse.ArgumentParser(description='Generate DDL based on data')
parser.add_argument('dialect', help='SQL dialect to output', type=str.lower)
parser.add_argument('datafile', help='Path to file storing data (accepts .yaml, .json)')
parser.add_argument('-i', '--inserts', action='store_true', help='also generate INSERT statements')
# parser.add_argument('-l', '--log', type=str, help='log level (CRITICAL, FATAL, ERROR, DEBUG, INFO, WARN)', default='INFO')
# parser.add_argument('-l', '--log', type=str, help='log level (CRITICAL, FATAL, ERROR, DEBUG, INFO, WARN)', default='INFO')
args = parser.parse_args()

"""
try:
    loglevel = int(getattr(logging, args.log.upper()))
except (AttributeError, TypeError) as e:
    raise NotImplementedError('log level "%s" not one of CRITICAL, FATAL, ERROR, DEBUG, INFO, WARN' % args.log)
"""

dialects = 'drizzle firebird mssql mysql oracle postgresql sqlite sybase'.split()

def generate():
    args = parser.parse_args()
    if args.dialect in ('pg', 'pgsql', 'postgres'):
        args.dialect = 'postgresql'
    if args.dialect not in dialects:
        raise NotImplementedError('First arg must be one of: %s' % ", ".join(dialects))
    table = Table(args.datafile)
    print(table.sql(args.dialect, args.inserts))
   