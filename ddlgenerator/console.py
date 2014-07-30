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
    parser.add_argument('datafile', help='Path to file storing data (accepts .yaml, .json)', nargs='+')
    parser.add_argument('-k', '--key', help='If primary key needed, name it this', type=str.lower)
    parser.add_argument('--force-key', help='Force every table to have a primary key',
                        action='store_true')
    parser.add_argument('-r', '--reorder', help='Reorder fields alphabetically, ``key`` first',
                        action='store_true')
    parser.add_argument('-u', '--uniques', action='store_true',
                        help='Include UNIQUE constraints where data is unique')
    parser.add_argument('-t', '--text', action='store_true',
                        help='Use variable-length TEXT columns instead of VARCHAR')

    parser.add_argument('-d', '--drops', action='store_true', help='Include DROP TABLE statements')
    parser.add_argument('-i', '--inserts', action='store_true', help='Include INSERT statements')
    parser.add_argument('--no-creates', action='store_true', help='Do not include CREATE TABLE statements')
    parser.add_argument('--limit', type=int, default=None, help='Max number of rows to read from each source file')
    parser.add_argument('--save-metadata-to', type=str, metavar='FILENAME',
                        help='Save table definition in FILENAME for later --use-saved-metadata run')
    parser.add_argument('--use-metadata-from', type=str, metavar='FILENAME',
                        help='Use metadata saved in FROM for table definition, do not re-analyze table structure')

    parser.add_argument('-l', '--log', type=str.upper,
                        help='log level (CRITICAL, FATAL, ERROR, DEBUG, INFO, WARN)', default='WARN')
    args = parser.parse_args()

    return args

def set_logging(args):
    try:
        loglevel = int(getattr(logging, args.log))
    except (AttributeError, TypeError) as e:
        raise NotImplementedError('log level "%s" not one of CRITICAL, FATAL, ERROR, DEBUG, INFO, WARN' %
                                  args.log)
    logging.getLogger().setLevel(loglevel)

def generate():
    args = read_args()
    set_logging(args)
    logging.info(str(args))
    if args.dialect in ('pg', 'pgsql', 'postgres'):
        args.dialect = 'postgresql'
    if args.dialect in ('dj', 'djan'):
        args.dialect = 'django'

    if args.dialect not in dialect_names:
        raise NotImplementedError('First arg must be one of: %s' % ", ".join(dialect_names))
    for datafile in args.datafile:
        table = Table(datafile, varying_length_text=args.text, uniques=args.uniques,
                      pk_name = args.key, force_pk=args.force_key, reorder=args.reorder,
                      save_metadata_to=args.save_metadata_to, metadata_source=args.use_metadata_from,
                      loglevel=args.log, limit=args.limit)
        if args.dialect.startswith('sqla'):
            print(table.sqlalchemy())
            if args.inserts:
                print("\n".join(table.inserts(dialect=args.dialect)))
                #inserter.compile().bindtemplate
        elif args.dialect.startswith('dj'):
            table.django_models()
        else:
            print(table.sql(dialect=args.dialect, inserts=args.inserts,
                            creates=(not args.no_creates), drops=args.drops,
                            metadata_source=args.use_metadata_from))

