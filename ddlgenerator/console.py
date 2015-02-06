import argparse
import logging
import re
try:
    from ddlgenerator.ddlgenerator import Table, dialect_names
    from ddlgenerator.ddlgenerator import sqla_head, sqla_inserter_call
    from ddlgenerator.ddlgenerator import emit_db_sequence_updates
except ImportError:
    from ddlgenerator import Table, dialect_names, sqla_head  # TODO: can py2/3 split this
    from ddlgenerator import sqla_head, sqla_inserter_call
    from ddlgenerator import emit_db_sequence_updates
# If anyone can explain these import differences to me, I will buy you a cookie.
from data_dispenser import sqlalchemy_table_sources

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
    parser.add_argument('-c', '--cushion', type=int, default=0, help='Extra length to pad column sizes with')
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

is_sqlalchemy_url = re.compile("^%s" % "|".join(dialect_names))

def generate_one(tbl, args, table_name=None):
    """
    Prints code (SQL, SQLAlchemy, etc.) to define a table.
    """
    table = Table(tbl, table_name=table_name, varying_length_text=args.text, uniques=args.uniques,
                  pk_name = args.key, force_pk=args.force_key, reorder=args.reorder, data_size_cushion=args.cushion,
                  save_metadata_to=args.save_metadata_to, metadata_source=args.use_metadata_from,
                  loglevel=args.log, limit=args.limit)
    if args.dialect.startswith('sqla'):
        if not args.no_creates:
            print(table.sqlalchemy())
        if args.inserts:
            print("\n".join(table.inserts(dialect=args.dialect)))
    elif args.dialect.startswith('dj'):
        table.django_models()
    else:
        print(table.sql(dialect=args.dialect, inserts=args.inserts,
                        creates=(not args.no_creates), drops=args.drops,
                        metadata_source=args.use_metadata_from))
    return table

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
    if args.dialect == 'sqlalchemy':
        print(sqla_head)
    for datafile in args.datafile:
        if is_sqlalchemy_url.search(datafile):
            table_names_for_insert = []
            for tbl in sqlalchemy_table_sources(datafile):
                t = generate_one(tbl, args, table_name=tbl.generator.name)
                if t.data:
                    table_names_for_insert.append(tbl.generator.name)
            if args.inserts and args.dialect == 'sqlalchemy':
                print(sqla_inserter_call(table_names_for_insert))
            if t and args.inserts:
                for seq_update in emit_db_sequence_updates(t.source.db_engine):
                    if args.dialect == 'sqlalchemy':
                        print('    conn.execute("%s")' % seq_update)
                    elif args.dialect == 'postgresql':
                        print(seq_update)
        else:
            generate_one(datafile, args)

