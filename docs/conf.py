# Cribbed from https://github.com/rtfd/readthedocs.org/issues/1139

import os
import subprocess
import sys

def run_apidoc(_):
    modules = ['tasks']
    for module in modules:
        cur_dir = os.path.abspath(os.path.dirname(__file__))
        output_path = os.path.join(cur_dir, module, 'doc')
        cmd_path = 'sphinx-apidoc'
        if hasattr(sys, 'real_prefix'):  # Check to see if we are in a virtualenv
            # If we are, assemble the path manually
            cmd_path = os.path.abspath(os.path.join(sys.prefix, 'bin', 'sphinx-apidoc'))
        subprocess.check_call([cmd_path, '-e', '-o', output_path, module, '--force'])

def setup(app):
    app.connect('builder-inited', run_apidoc)
