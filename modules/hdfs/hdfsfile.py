#!/usr/bin/python
# -*- coding: utf-8 -*-

# (c) 2017
# Grant Neale <grantneale@hotmail.com>
# Michael DeHaan <michael.dehaan@gmail.com>
#
# This module is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This module is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this module.  If not, see <http://www.gnu.org/licenses/>.

ANSIBLE_METADATA = {'metadata_version': '1.0',
                    'status': ['preview'],
                    'supported_by': 'community'}

DOCUMENTATION = '''
---
module: file
version_added: "2.2"
short_description: Sets attributes of files in HDFS.
description:
     - Sets attributes of files and directories, or removes
       files/directories in HDFS. M(hdfscopy) supports the same options as
       the C(hdfsfile) module.
notes:
    - See also M(hdfscopy)
requirements: [ "hdfs3 (Python HDFS client)" ]
author:
    - "Grant Neale"
options:
  path:
    description:
      - 'path to the file being managed.  Aliases: I(dest), I(name)'
    required: true
    default: []
    aliases: ['dest', 'name']
  state:
    description:
      - If C(directory), all immediate subdirectories will be created if they
        do not exist, they will be created with the supplied permissions.
        If C(file), the file will NOT be created if it does not exist, see the M(copy)
        module if you want that behavior.  If C(absent),
        directories will be deleted if empty, or recursively deleted only if C(recurse=true),
        and files or symlinks will be unlinked.
        Note that C(file) will not fail if the C(path) does not exist as the state did not change.
        If C(touchz), an empty file will be created if the C(path) does not
        exist, the access time of an existing empty file will be updated.  If C(path) is a directory
        or non-empty file, C(touchz) will return an error.
    required: false
    default: file
    choices: [ file, directory, touchz, absent ]
  recurse:
    required: false
    default: "no"
    choices: [ "yes", "no" ]
    description:
      - recursively set the specified file attributes or delete (applies only to state=directory)
'''

EXAMPLES = '''
# change file ownership, group and mode. When specifying mode using octal numbers, first digit should always be 0.
- file:
    path: /test/status
    owner: foo
    group: foo
    mode: 0644

# touch a file, using symbolic modes to set the permissions (equivalent to 0644)
- file:
    path: /test/status
    state: touchz
    mode: "u=rw,g=r,o=r"

# touch the same file, but add/remove some permissions
- file:
    path: /test/status
    state: touch
    mode: "u+rw,g-wx,o-rwx"

# create a directory if it doesn't exist
- file:
    path: /test/some_directory
    state: directory
    mode: 0755
'''

import os
import re

# import module snippets
from ansible.module_utils.basic import AnsibleModule
from ansible.module_utils.pycompat24 import get_exception
from ansible.module_utils._text import to_bytes, to_native

try:
    import hdfs3
    from hdfs3.compatibility import ConnectionError, FileNotFoundError, PermissionError
    HAS_HDFS3 = True
except ImportError:
    HAS_HDFS3 = False
    HAS_HDFS3_ERROR = get_exception()

# Can't use 07777 on Python 3, can't use 0o7777 on Python 2.4
PERM_BITS = int('07777', 8)       # file mode permission bits
EXEC_PERM_BITS = int('00111', 8)  # execute permission bits
DEFAULT_PERM = int('0666', 8)     # default file permission bits


def _get_hdfs_state(hdfs_client, b_path):
    """ Determine current state of a HDFS path """
    # Example hdfs_client.info(b_path) result:
    # {u'size': 0, u'kind': u'directory', u'group': u'supergroup', u'name': u'/test', u'last_mod': 1497242701,
    # u'replication': 0, u'owner': u'hadoop', u'last_access': 0, u'block_size': 0, u'permissions': 493}

    try:
        stat = hdfs_client.info(b_path)
        if stat['kind'] == 'directory':
            return 'directory'
        if stat['kind'] == 'file':
            return 'file'
    except FileNotFoundError:
        return 'absent'

    # Unhandled file type
    raise RuntimeError('Unhandled HDFS file type: %s' % stat['file_type'])


def _is_hdfs_dir(hdfs_client, b_path):
    """ Determine if a HDFS path is a directory """

    try:
        stat = hdfs_client.info(b_path)
        if stat['kind'] == 'directory':
            return True
    except FileNotFoundError:
        pass

    return False


def _is_hdfs_dir_empty(hdfs_client, b_path):
    """ Determine if a HDFS directory is empty is a directory """

    if not _is_hdfs_dir(hdfs_client, b_path):
        raise IOError('Path is not a directory: %s' % b_path)

    return len(hdfs_client.ls(b_path)) == 0


def _hdfs_exists(hdfs_client, b_path):
    """ Determine if a HDFS path exists """

    return hdfs_client.exists(b_path)


def _get_hdfs_user_and_group(hdfs_client, b_path):
    """ Determine current owner and group name of a HDFS path """

    stat = hdfs_client.info(b_path)
    return stat['owner'], stat['group']


def _get_hdfs_mode(hdfs_client, b_path):
    """ Determine current mode of a HDFS path """

    stat = hdfs_client.info(b_path)
    return stat['permissions']


def _recursive_set_attributes(module, hdfs_client, b_path, mode=None, owner=None, group=None):
    """
    Recursively set attributes on the children of the specified path (if a directory).
    
    Does not set attributes on the path itself
    """
    if not _is_hdfs_dir(hdfs_client, b_path):
        return False

    # root, dirs, files all contain absolute paths
    changed = False
    for root, dirs, files in _hdfs_walk(hdfs_client, b_path):
        for fsname in dirs + files:
            changed |= _set_hdfs_attributes_if_different(
                module, hdfs_client, fsname, mode, owner, group, changed
            )
    return changed


def _hdfs_walk(hdfs_client, top, topdown=True, onerror=None):
    """
    Directory tree generator for HDFS - based on os.walk().

    For each directory in the directory tree rooted at top (including top
    itself, but excluding '.' and '..'), yields a 3-tuple

        dirpath, dirnames, filenames

    Unlike os.walk() all contents of dirpath, dirnames and filenames are absolute paths.

    dirpath is a string, the path to the directory.  dirnames is a list of
    the paths of the subdirectories in dirpath (excluding '.' and '..').
    filenames is a list of the paths of the non-directory files in dirpath.

    If optional arg 'topdown' is true or not specified, the triple for a
    directory is generated before the triples for any of its subdirectories
    (directories are generated top down).  If topdown is false, the triple
    for a directory is generated after the triples for all of its
    subdirectories (directories are generated bottom up).

    When topdown is true, the caller can modify the dirnames list in-place
    (e.g., via del or slice assignment), and walk will only recurse into the
    subdirectories whose names remain in dirnames; this can be used to prune the
    search, or to impose a specific order of visiting.  Modifying dirnames when
    topdown is false is ineffective, since the directories in dirnames have
    already been generated by the time dirnames itself is generated. No matter
    the value of topdown, the list of subdirectories is retrieved before the
    tuples for the directory and its subdirectories are generated.

    By default errors from the os.listdir() call are ignored.  If
    optional arg 'onerror' is specified, it should be a function; it
    will be called with one argument, an os.error instance.  It can
    report the error to continue with the walk, or raise the exception
    to abort the walk.  Note that the filename is available as the
    filename attribute of the exception object.

    This method does not follow symbolic links to subdirectories on
    systems that support them.
    """

    # We may not have read permission for top, in which case we can't
    # get a list of the files the directory contains.  os.path.walk
    # always suppressed the exception then, rather than blow up for a
    # minor reason when (say) a thousand readable directories are still
    # left to visit.  That logic is copied here.

    try:
        children = hdfs_client.ls(top, detail=True)
    except (FileNotFoundError, PermissionError):
        e = get_exception()
        if onerror is not None:
            onerror(e)
        return

    dirs, nondirs = [], []
    for child in children:
        abs_path = child['name']
        if _is_hdfs_dir(hdfs_client, abs_path):
            dirs.append(abs_path)
        else:
            nondirs.append(abs_path)

    if topdown:
        yield top, dirs, nondirs
    for child_dir in dirs:
        for x in _hdfs_walk(hdfs_client, child_dir, topdown, onerror):
            yield x
    if not topdown:
        yield top, dirs, nondirs


def _set_hdfs_attributes_if_different(module, hdfs_client, path, mode, owner, group, changed, diff=None):
    # set modes and owners as needed
    b_path = to_bytes(path, errors='surrogate_or_strict')
    changed |= _set_owner_if_different(
        module, hdfs_client, b_path, owner, changed, diff
    )
    changed |= _set_group_if_different(
        module, hdfs_client, b_path, group, changed, diff
    )
    changed |= _set_mode_if_different(
        module, hdfs_client, b_path, mode, changed, diff
    )
    return changed


def _set_owner_if_different(module, hdfs_client, b_path, owner, changed, diff=None):

    if owner is None:
        return changed

    try:
        orig_owner, orig_group = _get_hdfs_user_and_group(hdfs_client, b_path)
    except (FileNotFoundError, PermissionError):
        e = get_exception()
        module.fail_json(path=b_path, msg='chown (owner) failed: failed to look up owner', details=str(e))

    if orig_owner != owner:

        if diff is not None:
            if 'before' not in diff:
                diff['before'] = {}
            diff['before']['owner'] = orig_owner
            if 'after' not in diff:
                diff['after'] = {}
            diff['after']['owner'] = owner

        if module.check_mode:
            return True

        # Update the owner
        try:
            hdfs_client.chown(b_path, owner=owner, group=None)
        except (FileNotFoundError, PermissionError):
            e = get_exception()
            module.fail_json(path=b_path, msg='chown (owner) failed', details=str(e))
        changed = True

    return changed


def _set_group_if_different(module, hdfs_client, b_path, group, changed, diff=None):

    if group is None:
        return changed

    try:
        orig_owner, orig_group = _get_hdfs_user_and_group(hdfs_client, b_path)
    except (FileNotFoundError, PermissionError):
        e = get_exception()
        module.fail_json(path=b_path, msg='chown (group) failed: failed to look up owner', details=str(e))

    if orig_group != group:

        if diff is not None:
            if 'before' not in diff:
                diff['before'] = {}
            diff['before']['group'] = orig_group
            if 'after' not in diff:
                diff['after'] = {}
            diff['after']['group'] = group

        if module.check_mode:
            return True

        # Update the group
        try:
            hdfs_client.chown(b_path, owner=None, group=group)
        except (FileNotFoundError, PermissionError):
            e = get_exception()
            module.fail_json(path=b_path, msg='chown (group) failed', details=str(e))
        changed = True

    return changed


def _set_mode_if_different(module, hdfs_client, b_path, mode, changed, diff=None):

    if mode is None:
        return changed

    try:
        prev_mode = _get_hdfs_mode(hdfs_client, b_path)
        is_dir = _is_hdfs_dir(hdfs_client, b_path)
    except (FileNotFoundError, PermissionError):
        e = get_exception()
        module.fail_json(path=b_path, msg="Unable to determine current mode", details=str(e))

    if not isinstance(mode, int):
        try:
            mode = int(mode, 8)
        except ValueError:
            try:
                mode = _symbolic_mode_to_octal(prev_mode, mode, is_dir)
            except ValueError:
                e = get_exception()
                module.fail_json(path=b_path,
                                 msg="mode must be in octal or symbolic form",
                                 details=str(e))

            if mode != os.stat.S_IMODE(mode):
                # prevent mode from having extra info orbeing invalid long number
                module.fail_json(path=b_path,
                                 msg="Invalid mode supplied, only permission info is allowed",
                                 details=mode)

    if prev_mode != mode:

        if diff is not None:
            if 'before' not in diff:
                diff['before'] = {}
            diff['before']['mode'] = '0%03o' % prev_mode
            if 'after' not in diff:
                diff['after'] = {}
            diff['after']['mode'] = '0%03o' % mode

        if module.check_mode:
            return True

        # Update mode
        try:
            hdfs_client.chmod(b_path, mode)
            new_mode = _get_hdfs_mode(hdfs_client, b_path)
        except (FileNotFoundError, PermissionError):
            e = get_exception()
            module.fail_json(path=b_path, msg='chmod failed', details=str(e))

        if new_mode != prev_mode:
            changed = True

    return changed


def _symbolic_mode_to_octal(prev_mode, symbolic_mode, is_dir):
    new_mode = prev_mode

    mode_re = re.compile(r'^(?P<users>[ugoa]+)(?P<operator>[-+=])(?P<perms>[rwxXst-]*|[ugo])$')
    for mode in symbolic_mode.split(','):
        match = mode_re.match(mode)
        if match:
            users = match.group('users')
            operator = match.group('operator')
            perms = match.group('perms')

            if users == 'a':
                users = 'ugo'

            for user in users:
                mode_to_apply = _get_octal_mode_from_symbolic_perms(prev_mode, is_dir, user, perms)
                new_mode = _apply_operation_to_mode(user, operator, mode_to_apply, new_mode)
        else:
            raise ValueError("bad symbolic permission for mode: %s" % mode)
    return new_mode


def _apply_operation_to_mode(user, operator, mode_to_apply, current_mode):
    if operator == '=':
        if user == 'u':
            mask = os.stat.S_IRWXU | os.stat.S_ISUID
        elif user == 'g':
            mask = os.stat.S_IRWXG | os.stat.S_ISGID
        elif user == 'o':
            mask = os.stat.S_IRWXO | os.stat.S_ISVTX

        # mask out u, g, or o permissions from current_mode and apply new permissions
        inverse_mask = mask ^ PERM_BITS
        new_mode = (current_mode & inverse_mask) | mode_to_apply
    elif operator == '+':
        new_mode = current_mode | mode_to_apply
    elif operator == '-':
        new_mode = current_mode - (current_mode & mode_to_apply)
    else:
        raise ValueError('Illegal argument: operator=%s' % operator)
    return new_mode


def _get_octal_mode_from_symbolic_perms(prev_mode, is_directory, user, perms):

    has_x_permissions = (prev_mode & EXEC_PERM_BITS) > 0
    apply_X_permission = is_directory or has_x_permissions

    # Permission bits constants documented at:
    # http://docs.python.org/2/library/stat.html#stat.S_ISUID
    if apply_X_permission:
        X_perms = {
            'u': {'X': os.stat.S_IXUSR},
            'g': {'X': os.stat.S_IXGRP},
            'o': {'X': os.stat.S_IXOTH}
        }
    else:
        X_perms = {
            'u': {'X': 0},
            'g': {'X': 0},
            'o': {'X': 0}
        }

    user_perms_to_modes = {
        'u': {
            'r': os.stat.S_IRUSR,
            'w': os.stat.S_IWUSR,
            'x': os.stat.S_IXUSR,
            's': os.stat.S_ISUID,
            't': 0,
            'u': prev_mode & os.stat.S_IRWXU,
            'g': (prev_mode & os.stat.S_IRWXG) << 3,
            'o': (prev_mode & os.stat.S_IRWXO) << 6 },
        'g': {
            'r': os.stat.S_IRGRP,
            'w': os.stat.S_IWGRP,
            'x': os.stat.S_IXGRP,
            's': os.stat.S_ISGID,
            't': 0,
            'u': (prev_mode & os.stat.S_IRWXU) >> 3,
            'g': prev_mode & os.stat.S_IRWXG,
            'o': (prev_mode & os.stat.S_IRWXO) << 3 },
        'o': {
            'r': os.stat.S_IROTH,
            'w': os.stat.S_IWOTH,
            'x': os.stat.S_IXOTH,
            's': 0,
            't': os.stat.S_ISVTX,
            'u': (prev_mode & os.stat.S_IRWXU) >> 6,
            'g': (prev_mode & os.stat.S_IRWXG) >> 3,
            'o': prev_mode & os.stat.S_IRWXO }
    }

    # Insert X_perms into user_perms_to_modes
    for key, value in X_perms.items():
        user_perms_to_modes[key].update(value)

    or_reduce = lambda mode, perm: mode | user_perms_to_modes[user][perm]
    return reduce(or_reduce, perms, 0)


def run(module, hdfs_client):

    params = module.params
    state = params['state']
    mode = params['mode']
    owner = params['owner']
    group = params['group']
    recurse = params['recurse']
    # TODO diff_peek = params['diff_peek']
    src = params['src']
    b_src = to_bytes(src, errors='surrogate_or_strict')

    # modify source as we later reload and pass, specially relevant when used by other modules.
    path = params['path']
    b_path = to_bytes(path, errors='surrogate_or_strict')

# TODO implement this - uesd by hdfscopy
#     # short-circuit for diff_peek
#     if diff_peek is not None:
#         appears_binary = False
#         try:
#             f = open(b_path, 'rb')
#             head = f.read(8192)
#             f.close()
#             if b("\x00") in head:
#                 appears_binary = True
#         except:
#             pass
#         module.exit_json(path=path, changed=False, appears_binary=appears_binary)

    prev_state = _get_hdfs_state(hdfs_client, b_path)
    if prev_state == 'link':
        module.fail_json(path=path,
                         msg='the specified path is currently a symlink.  hdfsfile module does not support symlinks')

    # state should default to file, but since that creates many conflicts,
    # default to 'current' when it exists.
    if state is None:
        if prev_state != 'absent':
            state = prev_state
        elif recurse:
            state = 'directory'
        else:
            state = 'file'

    # original_basename is used by other modules that depend on file.
    if state != 'absent' and _is_hdfs_dir(hdfs_client, b_path):
        basename = None
        if params['original_basename']:
            basename = params['original_basename']
        elif src is not None:
            basename = os.path.basename(src)
        if basename:
            params['path'] = path = os.path.join(path, basename)
            b_path = to_bytes(path, errors='surrogate_or_strict')

    # make sure the target path is a directory when we're doing a recursive operation
    if recurse and state not in ('directory', 'absent'):
        module.fail_json(path=path, msg="recurse option requires state to be either 'directory' or 'absent'")

    changed = False
    diff = {'before': {'path': path},
            'after': {'path': path},
            }

    state_change = False
    if prev_state != state:
        diff['before']['state'] = prev_state
        diff['after']['state'] = state
        state_change = True

    if state == 'absent':
        if state_change:
            if not module.check_mode:
                if prev_state == 'directory':
                    # Only do rm -r if recurse is true
                    if not recurse and not _is_hdfs_dir_empty(hdfs_client, b_path):
                        module.fail_json(msg="HDFS directory is non-empty, unable to delete with recurse=false: %s" % b_path)
                    try:
                        hdfs_client.rm(b_path, recursive=recurse)
                    except (PermissionError, FileNotFoundError, IOError):
                        e = get_exception()
                        module.fail_json(msg="Delete HDFS directory failed: %s" % str(e))
                else:
                    # rm
                    try:
                        hdfs_client.rm(b_path, recursive=False)
                    except (PermissionError, FileNotFoundError, IOError):
                        e = get_exception()
                        module.fail_json(msg="Delete HDFS file failed: %s" % str(e))
            module.exit_json(path=path, changed=True, diff=diff)
        else:
            module.exit_json(path=path, changed=False)

    elif state == 'file':
        if state_change:
            # file is not absent and any other state is a conflict
            module.fail_json(path=path, msg='file (%s) is %s, cannot continue' % (path, prev_state))

        changed = _set_hdfs_attributes_if_different(
            module, hdfs_client, path, mode, owner, group, changed, diff
        )
        module.exit_json(path=path, changed=changed, diff=diff)

    elif state == 'directory':
        if prev_state == 'absent':
            if module.check_mode:
                module.exit_json(changed=True, diff=diff)
            changed = True
            curpath = ''

            try:
                # Split the path so we can apply filesystem attributes recursively
                # from the root (/) directory for absolute paths or the base path
                # of a relative path.  We can then walk the appropriate directory
                # path to apply attributes.
                for dirname in path.strip('/').split('/'):
                    curpath = '/'.join([curpath, dirname])
                    # Remove leading slash if we're creating a relative path
                    if not os.path.isabs(path):
                        curpath = curpath.lstrip('/')
                    b_curpath = to_bytes(curpath, errors='surrogate_or_strict')

                    if not _hdfs_exists(hdfs_client, b_curpath):
                        hdfs_client.mkdir(b_curpath)
                        changed = _set_hdfs_attributes_if_different(
                            module, hdfs_client, curpath, mode, owner, group, changed, diff
                        )
            except Exception:
                e = get_exception()
                module.fail_json(path=path, msg='There was an issue creating directory %s: %s' % (curpath, str(e)))

        # We already know prev_state is not 'absent', therefore it exists in some form.
        elif prev_state != 'directory':
            module.fail_json(path=path, msg='%s already exists as a %s' % (path, prev_state))

        changed = _set_hdfs_attributes_if_different(
            module, hdfs_client, path, mode, owner, group, changed, diff
        )

        if recurse:
            changed |= _recursive_set_attributes(
                module, hdfs_client, to_bytes(path, errors='surrogate_or_strict'), mode, owner, group
            )

        module.exit_json(path=path, changed=changed, diff=diff)

    elif state =='link':
        module.fail_json(path=path, msg='hdfsfile module does not support creation of symlinks')

    elif state == 'touchz':
        if not module.check_mode:
            try:
                hdfs_client.touch(b_path)
            except (PermissionError, IOError):
                e = get_exception()
                module.fail_json(path=path,
                                 msg='Error, could not touchz target: %s' % to_native(e, nonstring='simplerepr'))

            try:
                _set_hdfs_attributes_if_different(
                    module, hdfs_client, path, mode, owner, group, True, diff)
            except SystemExit:
                e = get_exception()
                if e.code:
                    # We take this to mean that fail_json() was called from
                    # somewhere in basic.py
                    if prev_state == 'absent':
                        # If we just created the file we can safely remove it
                        try:
                            module.log('Attempting to cleanup touchz file: %s' % path)
                            hdfs_client.rm(b_path, recursive=False)
                        except (PermissionError, FileNotFoundError, IOError):
                            e_cleanup = get_exception()
                            module.log('Unable to cleanup touchz file: %s' % str(e_cleanup))
                        except FileNotFoundError:
                            # File was never created
                            pass
                raise e

        module.exit_json(dest=path, changed=True, diff=diff)

    module.fail_json(path=path, msg='unexpected position reached')


def main():

    module = AnsibleModule(
        argument_spec=dict(
            namenode_host=dict(required=True, type='str'),
            namenode_port=dict(required=False, default=8020, type='int'),
            effective_user=dict(required=False, default=None, type='str'),
            state=dict(choices=['file', 'directory', 'touchz', 'absent'], default=None),
            path=dict(aliases=['dest', 'name'], required=True, type='path'),
            mode=dict(required=False, default=None, type='raw'),
            owner=dict(required=False, default=None, type='str'),
            group=dict(required=False, default=None, type='str'),
            original_basename=dict(required=False),  # Internal use only, for recursive ops
            recurse=dict(default=False, type='bool'),
            diff_peek=dict(default=None),  # Internal use only, for internal checks in the action plugins
            validate=dict(required=False, default=None),  # Internal use only, for template and copy
            src=dict(required=False, default=None, type='path'),
        ),
        supports_check_mode=True
    )


    # Verify that the HDFS client library is available
    if not HAS_HDFS3:
        module.fail_json(msg="Failed to import required python module: hdfs3", details=str(HAS_HDFS3_ERROR))

    # Initialise HDFS client
    try:
        params = module.params
        hdfs_client = hdfs3.HDFileSystem(host=params['namenode_host'],
                                         port=params['namenode_port'],
                                         user=params['effective_user'])
        run(module, hdfs_client)
        hdfs_client.disconnect()
    except ConnectionError:
        ex = get_exception()
        module.fail_json(msg='Unable to init HDFS client for %s:%s: %s' % (
            params['namenode_port'], params['effective_user'], str(ex)))


if __name__ == '__main__':
    main()
