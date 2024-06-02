import sys
from contextlib import closing
from datetime import UTC, datetime

from rostmerge import database, export, ingest


def ingest_data(options):
    session_ts = datetime.now(tz=UTC)
    with closing(database.prepare_db(options.database)) as db:
        if options.ts:
            print(f'Initializing/updating database from file: {options.ts}')
            roster_data = ingest.read_csv_data(options.ts, ingest.TS_KEYS)
            missing = database.ts_insert_data(
                db,
                roster_data,
                return_missing_records=options.clear,
                timestamp=session_ts
            )
            if options.clear and missing:
                database.ts_delete_missing(missing)
        if options.usatf:
            print(f'Initializing/updating database from file: {options.usatf}')
            usatf_data = ingest.read_csv_data(options.usatf, ingest.USATF_KEYS)
            usatf_data = [u for u in usatf_data if u['usatf_id']]
            database.usatf_insert_data(db, usatf_data, session_ts)


def set_coaches(options):
    with closing(database.prepare_db(options.database)) as db:
        if options.clear:
            print('Clearing existing coach designations...')
            database.ts_clear_coaches(db)
            sys.exit(0)
        roster = database.ts_get_roster(db,
                                        exclude_coaches=False,
                                        exclude_matched=False,
                                        ordered=False)
        for (last, first, _, _, _, coach, *_) in roster:
            print(
                f'Is {first} {last} a coach (current: {coach == 1}): '
                '(Y)es, (N)o, (S)kip, (D)one'
            )
            while True:
                if choice := input('? ').upper():
                    if choice[0] in 'YN':
                        database.ts_set_coach(db, last, first, is_coach=choice[0] == 'Y')
                        break
                    elif choice[0] == 'S':
                        break
                    elif choice[0] == 'D':
                        return


def merge_usatf(options):
    with closing(database.prepare_db(options.database)) as db:
        if options.clear:
            print('Clearing existing USATF matches...')
            database.ts_clear_matches(db)
            sys.exit(0)
        roster = database.ts_get_roster(db,
                                        exclude_coaches=True,
                                        exclude_matched=True)
        print(f'Found {len(roster)} unmatched runners...')
        for runner in roster:
            last, first, dob, *_ = runner
            matches = database.usatf_find_match(db, runner)
            if matches.exact:
                if len(matches.exact) == 1:
                    print(f'Found exact match for {first} {last} ({dob})')
                    usatf_id, *_ = matches.exact[0]
                    print(usatf_id)
                    database.ts_set_usatf_id(db, last, first, usatf_id)
                else:
                    print(f'Found multiple exact matches for {first} {last} ({dob}):')
                    for i, (u_id, u_l, u_f, u_dob, u_gen, *_) in enumerate(matches.exact, start=1):
                        print(f'{i}) {u_l}, {u_f}, {u_dob}, {u_gen}, {u_id}')
                    while True:
                        if choice := input('#, (S)kip, (D)one? ').strip().upper():
                            if choice.isdigit() and 0 <= (i := int(choice) - 1) < len(matches.exact):
                                usatf_id, *_ = matches.exact[i]
                                print(usatf_id)
                                database.ts_set_usatf_id(db, last, first, usatf_id)
                                break
                            elif choice[0] == 'S':
                                break
                            elif choice[0] == 'D':
                                return
            elif matches.partial:
                print(f'Found partial match(es) for {first} {last} ({dob}):')
                for i, (u_id, u_l, u_f, u_dob, u_gen, *_) in enumerate(matches.partial, start=1):
                    print(f'{i}) {u_l}, {u_f}, {u_dob}, {u_gen}, {u_id}')
                while True:
                    if choice := input('#, (S)kip, (D)one? ').strip().upper():
                        if choice.isdigit() and 0 <= (i := int(choice) - 1) < len(matches.partial):
                            usatf_id, *_ = matches.partial[i]
                            print(usatf_id)
                            database.ts_set_usatf_id(db, last, first, usatf_id)
                            break
                        elif choice[0] == 'S':
                            break
                        elif choice[0] == 'D':
                            return


def export_roster(options):
    if not options.year:
        options.year = datetime.now(tz=UTC).year
    if not options.OUTPUT.endswith('.xlsx'):
        options.OUTPUT = options.OUTPUT + '.xlsx'
    with closing(database.prepare_db(options.database)) as db:
        roster = database.get_joint_runner_data(db)
        export.export_roster(options.OUTPUT, roster, options.year)


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='A program to merge TeamSnap and usatf roster information')
    parser.add_argument(
        '-d', '--database', type=str, default='roster.db',
        help='Roster database (defaults to "roster.db")'
    )
    subparsers = parser.add_subparsers(required=True)
    init = subparsers.add_parser('ingest', help='Initialize a database and/or ingest data')
    init.add_argument(
        '-t', '--ts', metavar='TS_ROSTER', type=str,
        help='A CSV file containing a roster exported from TeamSnap'
    )
    init.add_argument(
        '-u', '--usatf', metavar='USATF_DATA', type=str,
        help='A CSV file containing usatf membership and age verification data'
    )
    init.add_argument(
        '-c', '--clear', action='store_true',
        help='Clear entries not found in a newly ingested TeamSnap roster'
    )
    init.set_defaults(func=ingest_data)

    coach = subparsers.add_parser('coaches', help='Set (or clear) coaching entries')
    coach.add_argument('-c', '--clear', action='store_true', help='Clear all coaching flags and exit')
    coach.set_defaults(func=set_coaches)

    merge = subparsers.add_parser('merge', help='Merge usatf membership data')
    merge.add_argument('-c', '--clear', action='store_true', help='Clear existing matches and exit')
    merge.set_defaults(func=merge_usatf)

    export_s = subparsers.add_parser('export', help='Export the roster')
    export_s.add_argument('OUTPUT', type=str, help='Name of the exported roster file.')
    export_s.add_argument(
        '-y', '--year', type=int, default=None,
        help='Year to be used for calculating the age, defaults to the current year'
    )
    export_s.set_defaults(func=export_roster)

    options = parser.parse_args()
    if options.func == ingest_data and not (options.ts or options.usatf):
        sys.stderr.write('Error: `ingest` requires one or both of --ts or --usatf')
        sys.exit(-1)
    options.func(options)
