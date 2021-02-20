"""
This class converts an OpenLCA JSON-LD archive into a human-readable directory-
basically unzips the archive and pretty-prints all the JSON, with consistent sorting
so that version control diffs will operate.
"""

import json
import os
from shutil import rmtree

from zipfile import ZipFile


class OpenLcaGitter(object):
    _overwrite = None
    _delete = None

    def __init__(self, archive, target=None, overwrite=True, delete=True):
        """

        :param archive: path to the OpenLCA JSON-LD ZIP archive
        :param target:
        :param overwrite:
        :param delete:
        """
        self._archive = ZipFile(archive)
        self._target = target
        self.overwrite = overwrite
        self.delete = delete

    @property
    def overwrite(self):
        return bool(self._overwrite)

    @overwrite.setter
    def overwrite(self, value):
        self._overwrite = value

    @property
    def delete(self):
        return bool(self._delete)

    @delete.setter
    def delete(self, value):
        self._delete = value

    def _start(self):
        if os.path.exists(self._target):
            if self.delete:
                print('DELETING existing directory %s' % self._target)
                rmtree(self._target)
                os.makedirs(self._target)

            elif self.overwrite:
                print('Warning - path exists - may overwrite files')
        else:
            os.makedirs(self._target)

    def _check(self, name):
        p = os.path.join(self._target, name)
        if os.path.exists(p) and not self.overwrite:
            return False
        return p

    def _create_dirs(self):
        for f in self._archive.filelist:
            if f.is_dir():
                os.makedirs(os.path.join(self._target, f.filename), exist_ok=True)

    def _create_files(self):
        for f in self._archive.filelist:
            if f.is_dir():
                continue
            j = json.loads(self._archive.read(f.filename))
            self._create_root_file(f.filename, j)
            ''' # not sure whether we need to do any type-specific trickery... atm it seems no.
            try:
                typ, nam = f.filename.split('/', maxsplit=1)
            except ValueError:
                self._create_root_file(f.filename, j)
                continue
            '''

    def _create_root_file(self, name, j):
        dest = self._check(name)
        if dest:
            with open(dest, 'w') as fp:
                json.dump(j, fp, indent=2, sort_keys=True)

    def run(self, target=None):
        if target is None:
            if self._target is None:
                raise ValueError('Must supply a target directory')
        else:
            self._target = target
        self._start()
        self._create_dirs()
        self._create_files()
