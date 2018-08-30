#!/usr/bin/env python
#
# Public Domain 2014-2018 MongoDB, Inc.
# Public Domain 2008-2014 WiredTiger, Inc.
#
# This is free and unencumbered software released into the public domain.
#
# Anyone is free to copy, modify, publish, use, compile, sell, or
# distribute this software, either in source code form or as a compiled
# binary, for any purpose, commercial or non-commercial, and by any
# means.
#
# In jurisdictions that recognize copyright laws, the author or authors
# of this software dedicate any and all copyright interest in the
# software to the public domain. We make this dedication for the benefit
# of the public at large and to the detriment of our heirs and
# successors. We intend this dedication to be an overt act of
# relinquishment in perpetuity of all present and future rights to this
# software under copyright law.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
# OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
# ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
#
# test_backup09.py
#   Verify opening a backup cursor forces a log file switch.
#

import os, shutil
import wiredtiger, wttest
from wtscenario import make_scenarios

class test_backup09(wttest.WiredTigerTestCase):
    conn_config = 'config_base=false,create,log=(enabled)'
    uri = 'table:coll1'
    dir = 'backup.dir'

    types = [
        ('checkpoint', {'checkpoint': True}),
        ('no_checkpoint', {'checkpoint': False}),
    ]
    scenarios = make_scenarios(types)

    def data_and_start_backup(self):
        self.session.create(self.uri, 'key_format=i,value_format=i')

        cursor = self.session.open_cursor(self.uri)
        doc_id = 0

        for i in range(10):
            doc_id += 1
            cursor[doc_id] = doc_id

        if self.checkpoint:
            self.session.checkpoint()

        for i in range(10):
            doc_id += 1
            cursor[doc_id] = doc_id

        last_doc_in_backup = doc_id
        self.assertEqual(1, len(filter(lambda x: x.startswith('WiredTigerLog.'),
                                        os.listdir('.'))))
        backup_cursor = self.session.open_cursor('backup:')
        # Opening a backup cursor will immediately force a new log file.
        self.assertEqual(2, len(filter(lambda x: x.startswith('WiredTigerLog.'),
                                        os.listdir('.'))))

        for i in range(10):
            doc_id += 1
            cursor[doc_id] = doc_id

        cursor.close()
        # Flush to log to ensure all files contain all expected writes.
        # Otherwise a logical failure (containing more data than expected) may
        # go unnoticed.
        self.session.log_flush('sync=on')
        return backup_cursor, last_doc_in_backup

    def copy_and_restore(self, backup_cursor, last_expected_doc):
        log_files_to_copy = 0
        os.mkdir(self.dir)
        while True:
            ret = backup_cursor.next()
            if ret != 0:
                break
            shutil.copy(backup_cursor.get_key(), self.dir)
            if backup_cursor.get_key().startswith('WiredTigerLog.'):
                log_files_to_copy += 1

        self.assertEqual(ret, wiredtiger.WT_NOTFOUND)
        self.assertEqual(log_files_to_copy, 1)

        backup_conn = self.wiredtiger_open(self.dir, self.conn_config)
        session = backup_conn.open_session()
        cursor = session.open_cursor(self.uri)

        doc_cnt = 0
        for key, val in cursor:
            doc_cnt += 1
            self.assertLessEqual(key, last_expected_doc)

        self.assertEqual(doc_cnt, last_expected_doc)

    def test_backup_rotates_log(self):
        # Add some data, open a backup cursor, and add some more data. Return
        # the value of the last document that should appear on a restore.
        backup_cursor, last_doc = self.data_and_start_backup()

        # Copy the files returned via the backup cursor and bring up WiredTiger
        # on the destination. Verify no document later than last_doc exists.
        self.copy_and_restore(backup_cursor, last_doc)

if __name__ == '__main__':
    wttest.run()
