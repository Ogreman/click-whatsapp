#! /usr/bin/python
# -*- coding: utf-8 -*-
import sqlite3 as lite
import click
import os

# TODO: convert to click app
# TODO: stream with notifications (from db)
# TODO: input db location
DB_LOCATION = 'SOMETHING.db'
MESSAGE_TABLE = 'ZWAMESSAGE'
SESSION_TABLE = 'ZWACHATSESSION'


class Config(object):

    def __init__(self):
        self.verbose = False
        self.debug = False
        self.default_directory = "~/"
        self.default_output = ".whatsapp.sql"
        self.directory = ""
        self.mounted_db = ""
        self.sql = ""

    @property 
    def sql_path(self):
        return os.path.expanduser(
            "{self.directory}{sl}{self.sql}"
            .format(
                self=self,
                sl="" if self.directory.endswith('/') else "/",
            )
        )

    @property 
    def mounted_db_path(self):
        return os.path.expanduser(self.mounted_db)

    def write_data(self, data):
        try:
            with open(self.sql_path, 'w') as f:
                f.write(data)
        except (IOError, OSError):
            click.secho("Failed to write file!", fg="red")

    def read_data(self):
        try:
            helper = SQLHelper()
            with open(self.sql_path, 'r') as f:
                sql = f.read()
                return helper.import_messages_from_sql(sql)
        except (IOError, OSError):
            click.secho("Failed to read file!", fg="red")
            return
        except lite.OperationalError:
            click.secho("Failed to parse SQL!", fg="red")
            return

    def read_and_write(self):
        helper = SQLHelper(self.mounted_db_path)
        try:
            messages = list(helper.retrieve_messages_from_db())
        except lite.OperationalError:
            click.secho("Failed to retrieve from db!", fg="red")
            return
        else:
            if self.debug:
                click.secho(
                    'Retrieved {0} messages.'
                    .format(len(messages)),
                    fg="cyan"
                )
        try:
            data = helper.export_messages_to_sql(messages)
        except lite.OperationalError:
            click.secho("Failed to export messages as SQL!", fg="red")
            return
        else:
            if self.debug:
                click.secho('Exported messages to SQL.', fg="cyan")
        try:
            self.write_data(data.encode('utf-8'))
        except (TypeError, IOError, OSError):
            click.secho("Failed to write to file!", fg="red")
        else:
            if self.debug:
                click.secho('Wrote messages to file.', fg="cyan")
        return messages

    def read_from_db(self):
        try:
            return SQLHelper(self.mounted_db_path).retrieve_messages_from_db()
        except lite.OperationalError:
            click.secho("Failed to retrieve from db!", fg="red")


class SQLHelper(object):

    def __init__(self, db=None):
        self.con = lite.connect(db) if db is not None else None
        self.memcon = lite.connect(':memory:')
        self.table = 'Messages'

    @staticmethod
    def select(cur, table_name, what="*"):
        cur.execute("SELECT {0} FROM {1};".format(what, table_name))
        return cur.fetchall()
    
    def import_messages_from_sql(self, sql):
        with self.memcon:
            self.memcon.row_factory = lite.Row
            cur = self.memcon.cursor()
            cur.executescript(sql)
            return (
                (
                    row['Id'],
                    row['Session'],
                    row['Name'],
                    row['Text'],
                )
                for row in self.select(cur, self.table)
            )

    def retrieve_messages_from_db(self):
        with self.con:
        
            # ensure column names can be used
            self.con.row_factory = lite.Row
            cur = self.con.cursor()

            # build whatsapp session id -> name map
            sessions = { 
                row['Z_PK']: row['ZPARTNERNAME']
                for row in self.select(cur, SESSION_TABLE)
            }

            return (
                (
                    row['Z_PK'],
                    sessions[row['ZCHATSESSION']],
                    "me" if bool(row['ZISFROMME']) else row['ZPUSHNAME'] if row['ZPUSHNAME'] is not None else '',
                    row['ZTEXT'] if row['ZTEXT'] is not None else '',
                )
                for row in self.select(cur, MESSAGE_TABLE)
            )

    def export_messages_to_sql(self, messages):
        with self.memcon:
            cur = self.memcon.cursor()
            cur.execute("DROP TABLE IF EXISTS {table}"
                .format(table=self.table)
            )
            cur.execute(
                "CREATE TABLE {table}(Id INT, Session TEXT, Name TEXT, Text TEXT)"
                .format(table=self.table)
            )
            cur.executemany(
                "INSERT INTO {table} VALUES(?, ?, ?, ?)"
                .format(table=self.table),
                messages
            )
            return '\n'.join(self.memcon.iterdump())
    


pass_config = click.make_pass_decorator(Config, ensure=True)


@click.group()
@click.option('--verbose', is_flag=True)
@click.option('--debug', is_flag=True)
@click.option('--sdir', default=None)
@click.option('--output', default=None)
@click.option('--force', is_flag=True)
@click.option('--write', is_flag=True)
@click.option('--reverse', is_flag=True)
@pass_config
def cli(config, verbose, debug, sdir, output, force, write, reverse):
        
    config.verbose = verbose
    config.debug = debug
    config.directory = sdir or config.default_directory
    config.sql = output or config.default_output
    config.force = force
    config.write = write
    config.reverse = reverse

    if debug:
        click.secho(
            'Verbose set to {0}.'
            .format(config.verbose), 
            fg="cyan"
        )
        click.secho(
            'Debug set to {0}.'
            .format(config.debug), 
            fg="cyan"
        )
        if write:
            click.secho(
                'Output set to {0}.'
                .format(config.sql_path), 
                fg="cyan"
            )


@cli.command()
@click.argument('max-messages', default=-1)
@click.argument('session', default='')
@pass_config
def output(config, max_messages, session):
    
    def prompt():
        location = click.prompt(
            "Input ChatStorage location (enter 'd' to use default)",
            type=str
        )
        if location == "d":
            config.mounted_db = DB_LOCATION
        else:
            config.mounted_db = location

    if config.force:
        prompt()
        if config.write:
            click.echo("Parsing db file and dumping locally...")
            messages = config.read_and_write()
        else:
            click.echo("Parsing db file...")
            messages = config.read_from_db()
    elif os.path.exists(config.sql_path):
        click.echo("Reading existing data...")
        messages = config.read_data()
    else:
        prompt()
        if config.write:
            click.echo("Parsing db file and dumping locally...")
            messages = config.read_and_write()
        else:
            click.echo("Parsing db file...")
            messages = config.read_from_db()

    if messages is not None:
        if session:
            messages = filter(lambda m: m[1] == session, messages)
        if config.reverse:
            messages = reversed(list(messages))
        
        for count, message in enumerate(messages):
            if config.debug and config.verbose:
                click.secho("message #%s" % count, fg="cyan")
            click.secho(
                " ".join([
                    "(" + message[1] + ")",
                    message[2] + ":",
                    message[3]
                ]).encode('utf-8'),
                fg="green"
            )
            if count == max_messages:
                if config.verbose:
                    click.secho("Done %s messages" % max_messages, fg="cyan")
                break


