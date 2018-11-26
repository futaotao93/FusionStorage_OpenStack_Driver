# Copyright (c) 2018 Huawei Technologies Co., Ltd.
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

import base64
import os
import six

from defusedxml import ElementTree as ET
from oslo_log import log as logging

from cinder import exception
from cinder.i18n import _


LOG = logging.getLogger(__name__)


class FusionStorageConf(object):
    def __init__(self, conf):
        self.conf = conf
        self.last_modify_time = None

    def update_config_value(self):
        file_time = os.stat(self.conf.cinder_fusionstorage_conf_file).st_mtime
        if self.last_modify_time == file_time:
            return

        tree = ET.parse(self.conf.cinder_fusionstorage_conf_file)
        xml_root = tree.getroot()
        self._encode_authentication(tree, xml_root)

        attr_funcs = (
            self._san_address,
            self._san_user,
            self._san_password,
            self._pools_name,
        )
        for func in attr_funcs:
            func(xml_root)

    def _encode_authentication(self, tree, xml_root):
        name_node = xml_root.find('Storage/UserName')
        pwd_node = xml_root.find('Storage/UserPassword')

        need_encode = False
        if name_node is not None and not name_node.text.startswith('!$$$'):
            encoded = base64.b64encode(six.b(name_node.text)).decode()
            name_node.text = '!$$$' + encoded
            need_encode = True

        if pwd_node is not None and not pwd_node.text.startswith('!$$$'):
            encoded = base64.b64encode(six.b(pwd_node.text)).decode()
            pwd_node.text = '!$$$' + encoded
            need_encode = True

        if need_encode:
            tree.write(self.conf.cinder_fusionstorage_conf_file, 'UTF-8')

    def _assert_text_result(self, text, mess):
        if not text:
            msg = _("%s is not configured.") % mess
            LOG.error(msg)
            raise exception.InvalidInput(reason=msg)

    def _san_address(self, xml_root):
        address = xml_root.findtext('Storage/RestURL')
        self._assert_text_result(address, mess='RestURL')
        setattr(self.conf, 'san_address', address)

    def _san_user(self, xml_root):
        text = xml_root.findtext('Storage/UserName')
        self._assert_text_result(text, mess='UserName')
        user = base64.b64decode(six.b(text[4:])).decode()
        setattr(self.conf, 'san_user', user)

    def _san_password(self, xml_root):
        text = xml_root.findtext('Storage/UserPassword')
        self._assert_text_result(text, mess='UserPassword')
        pwd = base64.b64decode(six.b(text[4:])).decode()
        setattr(self.conf, 'san_password', pwd)

    def _pools_name(self, xml_root):
        pools_name = xml_root.findtext('Pool/StoragePool')
        self._assert_text_result(pools_name, mess='StoragePool')
        pools = set(x.strip() for x in pools_name.split(';') if x.strip())
        if not pools:
            msg = _('No valid storage pool configured.')
            LOG.error(msg)
            raise exception.InvalidInput(msg)
        setattr(self.conf, 'pools_name', list(pools))
