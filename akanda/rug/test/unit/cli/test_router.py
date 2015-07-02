from collections import namedtuple, deque
from cStringIO import StringIO
import unittest

import mock

from akanda.rug.cli import app
from akanda.rug.cli import router


class FunctionalTest(unittest.TestCase):

    def setUp(self):
        rug = namedtuple('rugcfg', [
            'admin_user', 'admin_password', 'admin_tenant_name', 'auth_url',
            'auth_strategy', 'auth_region', 'router_image_uuid'
        ])

        class RugController(app.RugController):

            def __init__(self, stdout):
                super(RugController, self).__init__(stdout=stdout)

            def initialize_app(self, argv):
                pass

            @property
            def rug_ini(self):
                return rug(**{
                    'admin_user': 'neutron',
                    'admin_password': 'secret',
                    'admin_tenant_name': 'service',
                    'auth_url': 'http://locahost',
                    'auth_strategy': 'keystone',
                    'auth_region': 'default',
                    'router_image_uuid': 'LATEST'
                })

        self.stdout = StringIO()
        self.app = RugController(stdout=self.stdout)

    def tearDown(self):
        del self.app


class TestRouterBatchedRebuild(FunctionalTest):

    def setUp(self):
        super(TestRouterBatchedRebuild, self).setUp()
        router.RouterBatchedRebuild.THREAD_SHUTDOWN.clear()

    def test_chunked(self):
        chunks_gen = router.RouterBatchedRebuild.chunked(range(50), 10)
        assert list(chunks_gen) == [
            range(0, 10),
            range(10, 20),
            range(20, 30),
            range(30, 40),
            range(40, 50),
        ]

    @mock.patch.object(router.RouterBatchedRebuild, 'neutron')
    @mock.patch.object(router.RouterBatchedRebuild, 'spawn_threads',
                       lambda self, threads: [])
    def test_no_routers(self, neutron):
        neutron.list_routers.return_value = {'routers': []}
        self.app.run(['--debug', 'batch', 'rebuild'])
        assert "routers to become ACTIVE" not in self.stdout.getvalue()

    @mock.patch.object(router.RouterBatchedRebuild, 'spawn_threads',
                       lambda self, threads: [])
    @mock.patch.object(router.RouterBatchedRebuild, 'get_instance', lambda *a:
                       None)
    @mock.patch.object(router.RouterBatchedRebuild, 'neutron')
    @mock.patch.object(router.RouterRebuild, 'take_action')
    def test_router_vm_is_missing(self, rebuild, neutron):
        r = {'id': '123', 'name': 'ak-456', 'status': 'ACTIVE'}
        neutron.list_routers.return_value = {'routers': [r]}

        with mock.patch.object(
            router.RouterBatchedRebuild, 'active',
            mock.Mock(__len__=lambda self: 1)
        ):
            self.app.run(['--debug', 'batch', 'rebuild'])

        assert "No VM found for 123" in self.stdout.getvalue()
        assert "Rebuilding 123 ak-456" in self.stdout.getvalue()
        assert rebuild.call_count == 1

    @mock.patch.object(router.RouterBatchedRebuild, 'spawn_threads',
                       lambda self, threads: [])
    @mock.patch.object(router.RouterBatchedRebuild, 'get_instance', lambda *a:
                       mock.Mock(image={'id': 'LATEST'}))
    @mock.patch.object(router.RouterBatchedRebuild, 'neutron')
    def test_router_image_is_latest(self, neutron):
        neutron.list_routers.return_value = {
            'routers': [{'id': '123', 'name': 'ak-456', 'status': 'ACTIVE'}]
        }
        self.app.run(['--debug', 'batch', 'rebuild'])
        assert "Router 123 is already up-to-date" in self.stdout.getvalue()

    @mock.patch.object(router.RouterBatchedRebuild, 'spawn_threads',
                       lambda self, threads: [])
    @mock.patch.object(router.RouterBatchedRebuild, 'get_instance', lambda *a:
                       mock.Mock(image={'id': 'OUT-OF-DATE'}))
    @mock.patch.object(router.RouterBatchedRebuild, 'neutron')
    @mock.patch.object(router.RouterRebuild, 'take_action')
    def test_router_out_of_date(self, rebuild, neutron):
        r = {'id': '123', 'name': 'ak-456', 'status': 'ACTIVE'}
        neutron.list_routers.return_value = {'routers': [r]}

        with mock.patch.object(
            router.RouterBatchedRebuild, 'active',
            mock.Mock(__len__=lambda self: 1)
        ):
            self.app.run(['--debug', 'batch', 'rebuild'])
        assert "Rebuilding 123 ak-456" in self.stdout.getvalue()
        assert rebuild.call_count == 1

    @mock.patch.object(router.RouterBatchedRebuild, 'neutron')
    @mock.patch.object(router.RouterBatchedRebuild, 'get_instance')
    def test_check_alive_vm_rebooting(self, get_instance, neutron):
        r = {'id': '123', 'name': 'ak-456', 'status': 'ACTIVE'}

        def gen_instance():
            for m in (
                mock.Mock(id='123', status='ACTIVE'),
                None,
                mock.Mock(id='456', status='DOWN'),
                mock.Mock(id='456', status='BUILD'),
            ):
                yield m

            router.RouterBatchedRebuild.THREAD_SHUTDOWN.set()
            yield mock.Mock(id='456', status='ACTIVE')

        _gen = gen_instance()
        get_instance.side_effect = lambda *a, **kw: _gen.next()

        neutron.show_router.return_value = {'router': r}
        router.RouterBatchedRebuild(self.app, [], '').check_alive([r], deque())
        assert neutron.show_router.call_count == 4

    @mock.patch.object(router.RouterBatchedRebuild, 'neutron')
    @mock.patch.object(router.RouterBatchedRebuild, 'get_instance')
    def test_check_alive_new_vm_booting(self, get_instance, neutron):
        r = {'id': '123', 'name': 'ak-456', 'status': 'ACTIVE'}

        def gen_instance():
            for m in (
                None,
                None,
                None,
                None,
                mock.Mock(id='123', status='DOWN'),
                mock.Mock(id='123', status='BUILD'),
            ):
                yield m

            router.RouterBatchedRebuild.THREAD_SHUTDOWN.set()
            yield mock.Mock(id='123', status='ACTIVE')

        _gen = gen_instance()
        get_instance.side_effect = lambda *a, **kw: _gen.next()

        neutron.show_router.return_value = {'router': r}
        router.RouterBatchedRebuild(self.app, [], '').check_alive([r], deque())
        assert neutron.show_router.call_count == 6

    @mock.patch.object(router.RouterBatchedRebuild, 'neutron')
    @mock.patch.object(router.RouterBatchedRebuild, 'get_instance')
    def test_check_alive_keyboard_interrupt(self, get_instance, neutron):
        r = {'id': '123', 'name': 'ak-456', 'status': 'DOWN'}

        def gen_instance():
            yield mock.Mock(id='123', status='DOWN')
            for m in range(9):
                yield None

            # Simulate a KeyboardInterrupt
            router.RouterBatchedRebuild.THREAD_SHUTDOWN.set()
            yield None

        _gen = gen_instance()
        get_instance.side_effect = lambda *a, **kw: _gen.next()

        neutron.show_router.return_value = {'router': r}
        router.RouterBatchedRebuild(self.app, [], '').check_alive([r], deque())
        assert neutron.show_router.call_count == 10
