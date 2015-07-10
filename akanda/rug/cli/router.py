# Copyright 2014 DreamHost, LLC
#
# Author: DreamHost, LLC
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.


"""Commands related to routers.
"""
from __future__ import print_function

import argparse
import logging
import multiprocessing
import subprocess
import sys
import thread
import threading
import time
from collections import namedtuple, deque

from akanda.rug import commands
from akanda.rug.cli import message
from akanda.rug.api import nova, quantum

from cliff import command
from neutronclient.v2_0 import client
from novaclient import exceptions
from oslo.config import cfg


class _TenantRouterCmd(message.MessageSending):

    def get_parser(self, prog_name):
        # Bypass the direct base class to let us put the tenant id
        # argument first
        p = super(_TenantRouterCmd, self).get_parser(prog_name)
        p.add_argument(
            'router_id',
        )
        return p

    def make_message(self, parsed_args):
        router_id = parsed_args.router_id.lower()
        if router_id == 'error':
            tenant_id = 'error'
        elif router_id == '*':
            tenant_id = '*'
        else:
            # Look up the tenant for a given router so we can send the
            # command using both and the rug can route it to the correct
            # worker. We do the lookup here instead of in the rug to avoid
            # having to teach the rug notification and dispatching code
            # about how to find the owner of a router, and to shift the
            # burden of the neutron API call to the client so the server
            # doesn't block. It also gives us a chance to report an error
            # when we can't find the router.
            n_c = client.Client(
                username=self.app.rug_ini.admin_user,
                password=self.app.rug_ini.admin_password,
                tenant_name=self.app.rug_ini.admin_tenant_name,
                auth_url=self.app.rug_ini.auth_url,
                auth_strategy=self.app.rug_ini.auth_strategy,
                region_name=self.app.rug_ini.auth_region,
            )
            response = n_c.list_routers(retrieve_all=True, id=router_id)
            try:
                router_details = response['routers'][0]
            except (KeyError, IndexError):
                raise ValueError('No router with id %r found: %s' %
                                 (router_id, response))
            assert router_details['id'] == router_id
            tenant_id = router_details['tenant_id']
        self.log.info(
            'sending %s instruction for tenant %r, router %r',
            self._COMMAND,
            tenant_id,
            router_id,
        )
        return {
            'command': self._COMMAND,
            'router_id': router_id,
            'tenant_id': tenant_id,
        }


class RouterUpdate(_TenantRouterCmd):
    """force-update a router"""

    _COMMAND = commands.ROUTER_UPDATE


class RouterRebuild(_TenantRouterCmd):
    """force-rebuild a router"""

    _COMMAND = commands.ROUTER_REBUILD

    def get_parser(self, prog_name):
        p = super(RouterRebuild, self).get_parser(prog_name)
        p.add_argument(
            '--router_image_uuid',
        )
        return p

    def take_action(self, parsed_args):
        uuid = parsed_args.router_image_uuid
        if uuid:
            nova_client = nova.Nova(cfg.CONF).client
            try:
                nova_client.images.get(uuid)
            except exceptions.NotFound:
                self.log.exception(
                    'could not retrieve custom image %s from Glance:' % uuid
                )
                raise
        return super(RouterRebuild, self).take_action(parsed_args)

    def make_message(self, parsed_args):
        message = super(RouterRebuild, self).make_message(parsed_args)
        message['router_image_uuid'] = parsed_args.router_image_uuid
        return message


class RouterDebug(_TenantRouterCmd):
    """debug a single router"""

    _COMMAND = commands.ROUTER_DEBUG


class RouterManage(_TenantRouterCmd):
    """manage a single router"""

    _COMMAND = commands.ROUTER_MANAGE


class RouterBatchedRebuild(command.Command):
    """rebuild every akanda router in batches"""

    THREAD_SHUTDOWN = threading.Event()
    Router = namedtuple('Router', ('id', 'name'))

    queue = deque()
    active = deque()

    def __init__(self, *args, **kw):
        logging.getLogger('urllib3').setLevel(logging.ERROR)
        super(RouterBatchedRebuild, self).__init__(*args, **kw)

    def cprint(self, msg, color='green'):
        try:
            import blessed
            term = blessed.Terminal()
            print(getattr(term, color)(msg), file=self.app.stdout)
        except ImportError:
            print(msg, file=self.app.stdout)

    @property
    def neutron(self):
        return client.Client(
            username=self.app.rug_ini.admin_user,
            password=self.app.rug_ini.admin_password,
            tenant_name=self.app.rug_ini.admin_tenant_name,
            auth_url=self.app.rug_ini.auth_url,
            auth_strategy=self.app.rug_ini.auth_strategy,
            region_name=self.app.rug_ini.auth_region,
        )

    @property
    def nova(self):
        return nova.Nova(cfg.CONF)

    def get_instance(self, router):
        router = RouterBatchedRebuild.Router(
            id=router['id'], name=router['name']
        )
        return self.nova.get_instance(router)

    def get_parser(self, prog_name):
        p = super(RouterBatchedRebuild, self).get_parser(prog_name)
        p.add_argument('--batch', action="store", type=int, default=15)
        return p

    @classmethod
    def chunked(cls, l, n):
        for i in xrange(0, len(l), n):
            yield l[i:i+n]

    def take_action(self, parsed_args):
        batch = parsed_args.batch

        logging.getLogger('akanda.rug.cli.message').setLevel(logging.ERROR)
        self.cprint("Restarting routers in batches of %s..." % batch, 'blue')

        threads = self.spawn_threads(batch)

        routers = self.neutron.list_routers()['routers']

        for i, chunk in enumerate(RouterBatchedRebuild.chunked(routers,
                                                               batch)):
            self.queue.clear()
            self.active.clear()
            rebooting = []
            for router in chunk:
                server = self.get_instance(router)
                if not server:
                    self.cprint("No VM found for %s!" % router['id'], 'red')
                elif self.app.rug_ini.router_image_uuid == server.image['id']:
                    self.cprint("Router %s is already up-to-date, skipping!"
                                % router['id'], 'green')
                    continue
                self.cprint(
                    "Rebuilding %s %s..." % (router['id'], router['name']),
                    'yellow'
                )
                rebooting.append(router)

            if not rebooting:
                continue

            self.queue.extend(rebooting)
            for router in rebooting:
                self.app.run(['--debug', 'router', 'rebuild', router['id']])

            total = len(rebooting)
            self.cprint(' '.join([
                '-'*25, '%s / %s' % (i, len(routers)), '-'*25
            ]), 'blue')
            self.cprint(
                "Waiting on %s routers to become ACTIVE....<Ctrl-C to skip>" %
                total,
                'blue'
            )
            try:
                while len(self.active) < len(rebooting):
                    new_total = len(rebooting) - len(self.active)
                    if total != new_total:
                        total = new_total
                        if total:
                            self.cprint(
                                "Waiting on %s routers to become "
                                "ACTIVE....<Ctrl-C to skip>" % total,
                                'blue'
                            )
                            time.sleep(5)
            except KeyboardInterrupt:
                try:
                    self.cprint(
                        "\nContinuing with next batch of routers...",
                        'blue'
                    )
                    continue
                except KeyboardInterrupt:
                    continue

        self.cprint("Waiting on worker threads to finish...", 'yellow')
        RouterBatchedRebuild.THREAD_SHUTDOWN.set()
        for t in threads:
            t.join()

    def spawn_threads(self, max_threads):
        """
        Spawn a number of worker threads to monitor Neutron routers for
        ALIVEness.
        """
        threads = []
        for i in range(
            min(max_threads, multiprocessing.cpu_count())
        ):
            t = threading.Thread(
                target=self.check_alive, args=(self.queue, self.active)
            )
            t.daemon = True
            t.start()
            threads.append(t)

        self.cprint('Spawned %d worker threads!' % len(threads), 'blue')
        return threads

    def check_alive(self, queue, active):
        """
        Thread worker that iterates over a queue of routers and waits for the
        underlying VM to change and become ACTIVE.

        :param queue: a collection of Neutron router dicts
        :type queue: collections.deque
        :param active: a thread-safe collections.deque used for tracking
                       routers which have successfully rebuilt
        :type active: collections.deque
        """
        logging.getLogger('urllib3').setLevel(logging.CRITICAL)

        def pop():
            router = None
            while router is None:
                if RouterBatchedRebuild.THREAD_SHUTDOWN.is_set():
                    return None, None
                try:
                    router = queue.pop()
                except IndexError:
                    time.sleep(1)
            return router, getattr(self.get_instance(router), 'id', None)

        router, old_uuid = pop()

        while router and not RouterBatchedRebuild.THREAD_SHUTDOWN.is_set():
            try:
                time.sleep(1)
                router = self.neutron.show_router(
                    router['id']).get('router', {})
                server = self.get_instance(router)
                if not server:  # VM exists
                    continue
                if server.status != 'ACTIVE':  # VM is ACTIVE
                    continue
                changed = old_uuid != server.id
                if not changed:  # VM uuid actually changed
                    continue
                if router['status'] != 'ACTIVE':  # Router is ACTIVE
                    continue
                self.cprint(
                    "%s is ACTIVE, new VM is %s" % (router['id'], server.id),
                    'green'
                )
                active.append(router)
                router, old_uuid = pop()
            except Exception as e:
                self.cprint(
                    'Thread %d encountered an error handling router %s: %s' % (
                        thread.get_ident(),
                        router['id'],
                        str(e)
                    ),
                    'red'
                )
                # put the router back into the work queue so another healthy
                # thread can grab it
                queue.append(router)
                continue

        self.cprint('Thread %d is exiting...' % thread.get_ident(), 'yellow')


class RouterSSH(_TenantRouterCmd):
    """ssh into a router over the management network"""

    def get_parser(self, prog_name):
        p = super(RouterSSH, self).get_parser(prog_name)
        p.add_argument('remainder', nargs=argparse.REMAINDER)
        return p

    def take_action(self, parsed_args):
        n_c = client.Client(
            username=self.app.rug_ini.admin_user,
            password=self.app.rug_ini.admin_password,
            tenant_name=self.app.rug_ini.admin_tenant_name,
            auth_url=self.app.rug_ini.auth_url,
            auth_strategy=self.app.rug_ini.auth_strategy,
            region_name=self.app.rug_ini.auth_region,
        )
        router_id = parsed_args.router_id.lower()
        ports = n_c.show_router(router_id).get('router', {}).get('ports', {})
        for port in ports:
            if port['fixed_ips'] and \
               port['device_owner'] == quantum.DEVICE_OWNER_ROUTER_MGT:
                v6_addr = port['fixed_ips'].pop()['ip_address']
                try:
                    cmd = ["ssh", "root@%s" % v6_addr] + parsed_args.remainder
                    subprocess.check_call(cmd)
                except subprocess.CalledProcessError as e:
                    sys.exit(e.returncode)
