import logging
import os


from fabric.api import env, task, run, local
from fabric.api import execute, cd, runs_once
from fabric.contrib.files import exists
from fabric.decorators import roles
from fabric.context_managers import prefix

# tasks are exported under the module names
from pylib import ec2
from pylib import cluster

import pylib

def run_python(*args, **kwargs):
    activate_virtual_env = \
        "source {venv}/bin/activate".format(
            venv=env.py_virtual_env)
    with prefix(activate_virtual_env):
        run(*args, **kwargs)


def init():
    logging.basicConfig(level=logging.INFO)
    environment()


def environment():
    if '__envset__' not in env:
        env.__envset__ = True
        env.use_ssh_config = True
        env.local_cwd = os.path.dirname(os.path.realpath(__file__))
        env.setdefault('remote_home', "/mnt/janus")
        env.setdefault('nfs_home', "/export/janus")
        env.setdefault('git_repo', "git@github.com:NYU-NEWS/janus.git")
        env.setdefault('git_revision', "origin/master")
        env.setdefault("py_virtual_env",
                       "{home}/py_venv".format(home=env.nfs_home))


@task
@runs_once
@roles('leaders')
def create_virtual_env():
    if exists(env.py_virtual_env):
        return
    with cd(env.nfs_home):
        execute('retrieve_code')
        run('pyenv local 2.7.11')
        run('virtualenv {venv}'.format(venv=env.py_virtual_env))
        run('{venv}/bin/pip install -r requirements.txt'.format(venv=env.py_virtual_env))


@task
@runs_once
@roles('leaders')
def build(args=None, clean=True):
    execute('retrieve_code')
    execute('create_virtual_env')
    sudo('apt-get install pkg-config')
    opts = ["./waf"] 
    if args:
        opts.insert(1, args)
    if clean:
        opts.extend(["distclean", "configure"])
    opts.append("build")
    with cd(env.nfs_home): 
        run_python(' '.join(opts))


@task
@runs_once
@roles('leaders')
def retrieve_code():
    parent = os.path.dirname(env.nfs_home)
    with cd(parent):
        logging.info("check out code in {}".format(parent))
        if not exists(env.nfs_home):
            run('git clone --recursive --depth=1 ' + env.git_repo)
        else:
            with cd(env.nfs_home):
                run('git fetch origin')
                run('git reset --hard {rev}'.format(rev=env.git_revision))


init()