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

import json
from oslo_config import cfg
from oslo_log import log as logging

from cinder import exception
from cinder.i18n import _
from cinder import interface
from cinder.volume import driver
from cinder.volume.drivers.fusionstorage import constants
from cinder.volume.drivers.fusionstorage import fs_client
from cinder.volume.drivers.fusionstorage import fs_conf
from cinder.volume import utils as volume_utils

LOG = logging.getLogger(__name__)

fusionstorage_opts = [
    cfg.StrOpt('cinder_fusionstorage_conf_file',
               default='/etc/cinder/cinder_fusionstorage_conf.xml',
               help='The configuration file for '
                    'the Cinder fusion storage driver.'),
    cfg.DictOpt('hosts',
                default={},
                help='The manager ip for different hosts.'),
]

CONF = cfg.CONF
CONF.register_opts(fusionstorage_opts)


@interface.volumedriver
class DSWAREDriver(driver.VolumeDriver):

    def __init__(self, *args, **kwargs):
        super(DSWAREDriver, self).__init__(*args, **kwargs)

        if not self.configuration:
            msg = _('Configuration is not found.')
            LOG.error(msg)
            raise exception.InvalidInput(reason=msg)

        self.configuration.append_config_values(fusionstorage_opts)
        self.conf = fs_conf.FusionStorageConf(self.configuration)
        self.count = constants.ZERO
        self.client = None

    def do_setup(self, context):
        self.conf.update_config_value()
        url_str = self.configuration.san_address
        url_user = self.configuration.san_user
        url_password = self.configuration.san_password

        self.client = fs_client.RestCommon(
            fs_address=url_str, fs_user=url_user,
            fs_password=url_password)
        self.client.login()

    def check_for_setup_error(self):
        all_pools = self.client.query_pool_info()
        all_pools_name = [p['poolName'] for p in all_pools
                          if p.get('poolName')]

        for pool in self.configuration.pools_name:
            if pool not in all_pools_name:
                msg = _('Storage pool %(pool)s does not exist '
                        'in the FusionStorage.') % {'pool': pool}
                LOG.error(msg)
                raise exception.InvalidInput(reason=msg)

    def _update_pool_stats(self):
        backend_name = self.configuration.safe_get(
            'volume_backend_name') or self.__class__.__name__
        data = {"volume_backend_name": backend_name,
                "driver_version": "2.0.9",
                "QoS_support": False,
                "thin_provisioning_support": False,
                "pools": []
                }
        all_pools = self.client.query_pool_info()

        for pool in all_pools:
            if pool['poolName'] in self.configuration.pools_name:
                single_pool_info = self._update_single_pool_info_status(pool)
                data['pools'].append(single_pool_info)
        return data

    def _get_capacity(self, pool_info):
        pool_capacity = {}

        total = float(pool_info['totalCapacity']) / constants.CAPACITY_UNIT
        free = (float(pool_info['totalCapacity']) -
                float(pool_info['usedCapacity'])) / constants.CAPACITY_UNIT
        pool_capacity['total_capacity_gb'] = total
        pool_capacity['free_capacity_gb'] = free

        return pool_capacity

    def _update_single_pool_info_status(self, pool_info):
        status = {}
        capacity = self._get_capacity(pool_info=pool_info)
        status.update({
            "pool_name": pool_info['poolName'],
            "total_capacity_gb": capacity['total_capacity_gb'],
            "free_capacity_gb": capacity['free_capacity_gb'],
        })
        return status

    def get_volume_stats(self, refresh=False):
        self.client.keep_alive()
        stats = self._update_pool_stats()
        return stats

    def _check_volume_exist(self, volume):
        vol_name = self._get_vol_name(volume)
        result = self.client.query_volume_by_name(vol_name=vol_name)
        if result:
            return True
        else:
            return False

    def _assert_check_result(self, msg):
        LOG.error(msg)
        raise exception.VolumeBackendAPIException(data=msg)

    def _get_pool_id(self, volume):
        pool_id = None
        pool_name = volume_utils.extract_host(volume.host, level='pool')
        all_pools = self.client.query_pool_info()
        for pool in all_pools:
            if pool_name == pool['poolName']:
                pool_id = pool['poolId']

        if pool_id is None:
            msg = _('Storage pool %(pool)s does not exist and may be manually '
                    'deleted. Please check.') % {"pool": pool_id}
            LOG.error(msg)
            raise exception.InvalidInput(reason=msg)
        return pool_id

    def _get_vol_name(self, volume):
        provider_location = volume.get("provider_location", None)
        if provider_location:
            vol_name = json.loads(provider_location).get("name")
        else:
            vol_name = volume.name
        return vol_name

    def create_volume(self, volume):
        pool_id = self._get_pool_id(volume)
        vol_name = volume.name
        vol_size = volume.size
        vol_size *= constants.CONVERSION
        self.client.create_volume(
            pool_id=pool_id, vol_name=vol_name, vol_size=vol_size)

    def delete_volume(self, volume):
        vol_name = self._get_vol_name(volume)
        if self._check_volume_exist(volume):
            self.client.delete_volume(vol_name=vol_name)

    def extend_volume(self, volume, new_size):
        vol_name = self._get_vol_name(volume)
        if not self._check_volume_exist(volume):
            msg = _("Volume: %(vol_name)s does not exist!"
                    ) % {"vol_name": vol_name}
            self._assert_check_result(msg)
        elif volume.size >= new_size:
            msg = _("Extend volume failed! New size %(size)s "
                    "should be greater than old size %(vol_size)s!"
                    ) % {"size": new_size, "vol_size": volume.size}
            raise exception.VolumeBackendAPIException(data=msg)
        else:
            new_size *= constants.CONVERSION
            self.client.expand_volume(vol_name, new_size)

    def _check_snapshot_exist(self, volume, snapshot):
        pool_id = self._get_pool_id(volume)
        snapshot_name = self._get_snapshot_name(snapshot)
        result = self.client.query_snapshot_list(
            pool_id=pool_id, snapshot_name=snapshot_name)
        if result.get('totalNum'):
            return True
        else:
            return False

    def _get_snapshot_name(self, snapshot):
        provider_location = snapshot.get("provider_location", None)
        if provider_location:
            snapshot_name = json.loads(provider_location).get("name")
        else:
            snapshot_name = snapshot.name
        return snapshot_name

    def create_volume_from_snapshot(self, volume, snapshot):
        vol_name = self._get_vol_name(volume)
        snapshot_name = self._get_snapshot_name(snapshot)
        vol_size = volume.size

        if not self._check_snapshot_exist(snapshot.volume, snapshot):
            msg = _("Snapshot: %(name)s does not exist!"
                    ) % {"name": snapshot_name}
            self._assert_check_result(msg)
        elif self._check_volume_exist(volume):
            msg = _("Volume: %(vol_name)s already exists!"
                    ) % {'vol_name': vol_name}
            self._assert_check_result(msg)
        else:
            vol_size *= constants.CONVERSION
            self.client.create_volume_from_snapshot(
                snapshot_name=snapshot_name, vol_name=vol_name,
                vol_size=vol_size)

    def create_cloned_volume(self, volume, src_volume):
        vol_name = self._get_vol_name(volume)
        src_vol_name = self._get_vol_name(src_volume)

        vol_size = volume.size
        vol_size *= constants.CONVERSION

        if not self._check_volume_exist(src_volume):
            msg = _("Volume: %(vol_name)s does not exist!"
                    ) % {"vol_name": src_vol_name}
            self._assert_check_result(msg)
        else:
            self.client.create_volume_from_volume(
                vol_name=vol_name, vol_size=vol_size,
                src_vol_name=src_vol_name)

    def create_snapshot(self, snapshot):
        snapshot_name = self._get_snapshot_name(snapshot)
        vol_name = self._get_vol_name(snapshot.volume)

        self.client.create_snapshot(
            snapshot_name=snapshot_name, vol_name=vol_name)

    def delete_snapshot(self, snapshot):
        snapshot_name = self._get_snapshot_name(snapshot)

        if self._check_snapshot_exist(snapshot.volume, snapshot):
            self.client.delete_snapshot(snapshot_name=snapshot_name)

    def _query_volume_by_id(self, pool_id, vol_id):
        vols_info = self.client.query_all_volumes(pool_id)
        for info in vols_info:
            if int(info.get('volId')) == int(vol_id):
                return info
        return None

    def _get_volume_info(self, pool_id, existing_ref):
        vol_name = existing_ref.get('source-name')
        vol_id = existing_ref.get('source-id')

        if not (vol_name or vol_id):
            msg = _('Must specify source-name or source-id.')
            raise exception.ManageExistingInvalidReference(
                existing_ref=existing_ref, reason=msg)

        vol_info = None
        if vol_name:
            vol_info = self.client.query_volume_by_name(vol_name)

        if vol_id:
            try:
                vol_info = self.client.query_volume_by_id(vol_id)
            except Exception:
                LOG.warning("Query volume info through id failed!")
                vol_info = self._query_volume_by_id(pool_id, vol_id)

        if not vol_info:
            msg = _("Can't find volume on the array, please check the "
                    "source-name or source-id.")
            raise exception.ManageExistingInvalidReference(
                existing_ref=existing_ref, reason=msg)
        return vol_info

    def manage_existing(self, volume, existing_ref):
        pool = self._get_pool_id(volume)
        vol_info = self._get_volume_info(pool, existing_ref)
        vol_pool_id = vol_info.get('poolId')

        if pool != vol_pool_id:
            msg = (_("The specified LUN does not belong to the given "
                     "pool: %s.") % pool)
            raise exception.ManageExistingInvalidReference(
                existing_ref=existing_ref, reason=msg)

        provider_location = {}
        provider_location['name'] = vol_info.get('volName')

        return {'provider_location': json.dumps(provider_location)}

    def manage_existing_get_size(self, volume, existing_ref):
        pool = self._get_pool_id(volume)
        vol_info = self._get_volume_info(pool, existing_ref)
        remainder = float(vol_info.get("volSize")) % constants.CONVERSION

        if remainder != 0:
            msg = _("The volume size must be an integer multiple of 1 GB.")
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        size = float(vol_info.get("volSize")) / constants.CONVERSION
        return int(size)

    def unmanage(self, volume):
        return

    def _get_snapshot_info(self, volume, existing_ref):
        snapshot_name = existing_ref.get('source-name')

        if not snapshot_name:
            msg = _("Can't find volume on the array, please check the "
                    "source-name.")
            raise exception.ManageExistingInvalidReference(
                existing_ref=existing_ref, reason=msg)

        pool_id = self._get_pool_id(volume)
        snapshot_info = self.client.query_snapshot_list(
            pool_id, snapshot_name=snapshot_name)

        if snapshot_info.get('totalNum'):
            snapshot_info = snapshot_info.get('snapshotList')[0]
        else:
            msg = _("Can't find snapshot on the array.")
            raise exception.ManageExistingInvalidReference(
                existing_ref=existing_ref, reason=msg)

        return snapshot_info

    def _check_snapshot_match_volume(self, vol_name, snapshot_name):
        snapshot_info = self.client.query_snapshot_by_volume(
            vol_name, snapshot_name=snapshot_name)

        if snapshot_info:
            return True
        else:
            return False

    def manage_existing_snapshot(self, snapshot, existing_ref):
        volume = snapshot.volume
        snapshot_info = self._get_snapshot_info(volume, existing_ref)

        vol_name = self._get_vol_name(volume)
        if not self._check_snapshot_match_volume(
                vol_name, snapshot_info.get("snapName")):
            msg = (_("The specified snapshot does not belong to the given "
                     "volume: %s.") % vol_name)
            raise exception.ManageExistingInvalidReference(
                existing_ref=existing_ref, reason=msg)

        provider_location = {}
        provider_location['name'] = snapshot_info.get('snapName')
        return {'provider_location': json.dumps(provider_location)}

    def manage_existing_snapshot_get_size(self, snapshot, existing_ref):
        snapshot_info = self._get_snapshot_info(snapshot.volume, existing_ref)
        remainder = float(snapshot_info.get("snapSize")) % constants.CONVERSION

        if remainder != 0:
            msg = _("The snapshot size must be an integer multiple of 1 GB.")
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)
        size = float(snapshot_info.get("snapSize")) / constants.CONVERSION
        return int(size)

    def unmanage_snapshot(self, snapshot):
        return

    def _get_manager_ip(self, context):
        if self.configuration.safe_get(constants.HOSTS) is None:
            msg = _("The configuration file does not contain the hosts.")
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(msg)

        if self.configuration.safe_get(constants.HOSTS).get(context['host']):
            return self.configuration.safe_get(constants.HOSTS).get(
                context['host'])
        else:
            msg = _("The required host: %(host)s and its manager ip are not "
                    "included in the configuration file."
                    ) % {"host": context['host']}
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(msg)

    def _attach_volume(self, context, volume, properties, remote=False):
        vol_name = self._get_vol_name(volume)
        if not self._check_volume_exist(volume):
            msg = _("Volume: %(vol_name)s does not exist!"
                    ) % {"vol_name": vol_name}
            self._assert_check_result(msg)
        manager_ip = self._get_manager_ip(properties)
        result = self.client.attach_volume(vol_name, manager_ip)
        attach_path = result[vol_name][0]['devName'].encode('unicode-escape')
        attach_info = dict()
        attach_info['device'] = dict()
        attach_info['device']['path'] = attach_path
        if attach_path == '':
            msg = _("Host attach volume failed!")
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)
        return attach_info, volume

    def _detach_volume(self, context, attach_info, volume, properties,
                       force=False, remote=False, ignore_errors=False):
        vol_name = self._get_vol_name(volume)
        if self._check_volume_exist(volume):
            manager_ip = self._get_manager_ip(properties)
            self.client.detach_volume(vol_name, manager_ip)

    def initialize_connection(self, volume, connector):
        vol_name = self._get_vol_name(volume)
        manager_ip = self._get_manager_ip(connector)
        if not self._check_volume_exist(volume):
            msg = _("Volume: %(vol_name)s does not exist!"
                    ) % {"vol_name": vol_name}
            self._assert_check_result(msg)
        self.client.attach_volume(vol_name, manager_ip)
        volume_info = self.client.query_volume_by_name(vol_name=vol_name)
        vol_wwn = volume_info.get('wwn')
        by_id_path = "/dev/disk/by-id/" + "wwn-0x%s" % vol_wwn
        properties = {'device_path': by_id_path}
        return {'driver_volume_type': 'local',
                'data': properties}

    def terminate_connection(self, volume, connector, **kwargs):
        if self._check_volume_exist(volume):
            manager_ip = self._get_manager_ip(connector)
            vol_name = self._get_vol_name(volume)
            self.client.detach_volume(vol_name, manager_ip)

    def create_export(self, context, volume, connector):
        pass

    def ensure_export(self, context, volume):
        pass

    def remove_export(self, context, volume):
        pass
