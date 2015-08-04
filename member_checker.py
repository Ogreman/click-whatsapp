#! /usr/bin/python

import requests
import json
import click
import os
requests.packages.urllib3.disable_warnings()

API_KEY = os.environ.get('MEETUP_API_KEY', None)
URL = "https://api.meetup.com/2/members?offset=0&sign=True&format=json&group_urlname={url}&order=joined&key={key}"


class Config(object):

    def __init__(self):
        self.verbose = False
        self.debug = False
        self.default_directory = "~/"
        self.directory = ""

    def path(self, name=None):
        if name is not None:
            return os.path.expanduser(
                "{dir}{sl}.{name}.mch"
                .format(
                    dir=self.directory,
                    name=name,
                    sl="" if self.directory.endswith('/') else "/",
                )
            )
        else:
            return os.path.expanduser(self.directory)

pass_config = click.make_pass_decorator(Config, ensure=True)


@click.group()
@click.option('--verbose', is_flag=True)
@click.option('--debug', is_flag=True)
@click.option('--sdir', default=None, help="This is the directory to use for storage.")
@pass_config
def cli(config, verbose, debug, sdir):
    if API_KEY is None:
        raise click.ClickException('API_KEY not set!')
        
    config.verbose = verbose
    config.debug = debug
    config.directory = sdir or config.default_directory

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
        click.secho(
            'Directory set to {0}.'
            .format(config.directory), 
            fg="cyan"
        )


@cli.command()
@click.argument('name', default='')
@click.argument('url', default='')
@pass_config
def check(config, name, url):
    if config.verbose:
        click.echo(
            "Checking members of {name} meetup..."
            .format(name=name)
        )
    checker = MemberChecker(name=name, url=url, path=config.path(name))
    
    try:
        if config.verbose:
            click.echo("Checking for existing member data...")
        checker.load()
    except (TypeError, IOError, OSError):
        if config.debug:
            click.secho("Failed to load member data from file.", fg="red")
    else:
        if config.verbose:
            click.secho("Read member data from file.", fg="green")
        click.echo("{name}: {number} existing members.".format(
            name=name, 
            number=len(checker)
        ))
    if not checker.url:
        checker.url = click.prompt(
            'Enter Meetup group URL for {name}'.format(name=name),
            type=str
        )
    
    try:
        number, newbies, leavers = checker.check()
        click.echo("{}: {} received members.".format(name, number))
        if newbies:
            click.echo(
                "{name}: Newbies ({n}): {l}".format(
                    name=name, 
                    n=len(newbies), 
                    l=[m[1] for m in newbies]
                )
            )
        if leavers: 
            click.echo(
                "{name}: Leavers ({n}): {l}".format(
                    name=name, 
                    n=len(leavers), 
                    l=[m[1] for m in leavers]
                )
            )
        if not newbies and not leavers:
            click.echo("{name}: No change.".format(name=name))
    except CheckError:
        click.secho("Failed to get new member data from Meetup.", fg="red")
    else:
        if config.verbose:
            click.secho("Finished checking member data.", fg="green")
        try:
            checker.dump_last()
        except CheckError:
            click.secho("Member data missing.", fg="red")
        except (TypeError, IOError, OSError):
            click.secho("Failed to save member data to file.", fg="red")
        else:
            if config.verbose:
                click.secho("Saved member data to file.", fg="green")
    
    if config.verbose:
        click.echo("Done.")


class CheckError(Exception): pass


class MemberChecker(object):    

    def __init__(self, name, url='', path=''): 
        self.name = name
        self._group_url = url
        self._member_set = {}
        self._url = ''
        self._last_results = None
        self._path = path

    @property
    def url(self):
        if self._group_url:
            self._url = URL.format(url=self._group_url, key=API_KEY)
        return self._url

    @url.setter
    def url(self, value):
        self._group_url = value

    def dump_last(self):
        if self._last_results is not None:
            self.dump(self._last_results)
        else:
            raise CheckError('Missing results.')

    def dump(self, results):
        data = {'url': self._group_url, 'results': results}
        with open(self._path, 'wb') as fh:
            fh.write(json.dumps(data))

    def load(self):
        with open(self._path, 'rb') as fh:
            data = json.loads(fh.read())
            results = data['results']
            self._group_url = data['url']
        self._member_set = self.reformat(results)

    @staticmethod
    def reformat(data):
        return { (r['id'], r['name']) for r in data }

    @property 
    def members(self):
        return self._member_set

    def check(self):
        url = self.url
        results = []

        while True:
            try:
                resp = requests.get(url)
                if resp.status_code != 200:
                    raise CheckError('Failed to get data.')
                json_response = resp.json()
            except (AttributeError, requests.exceptions.ConnectionError) as e:
                raise CheckError('Failed to get data.')
            results = results + json_response['results']
            if json_response['meta']['next']:
                url = json_response['meta']['next']
            else:
                break

        new_set = self.reformat(results)

        if self.members:
            newbies = new_set - self.members
            leavers = self.members - new_set
        else:
            newbies, leavers = None, None
        self._member_set = new_set
        self._last_results = results
        return len(new_set), newbies, leavers

    def __len__(self):
        return len(self._member_set)
