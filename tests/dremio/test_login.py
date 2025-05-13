import pytest
from tests.testutils.container import DremioContainer, InitUser

from src.dremio import Dremio, AuthError


@pytest.fixture(scope="module", autouse=True)
def module_user() -> InitUser:
    return InitUser(userName="test", password="Test123!")


@pytest.fixture(scope="module", autouse=True)
def module_container(module_user: InitUser):
    container = DremioContainer(module_user)
    yield container.start()
    container.stop()


def test_login_success(module_container: DremioContainer, module_user: InitUser):
    hostname = module_container.get_container_host_ip()
    port = module_container.get_exposed_port(9047)
    dremio = Dremio(hostname=hostname, port=int(port), protocol="http")
    me = dremio.login(username=module_user.userName, password=module_user.password)
    assert me.userName == module_user.userName


def test_login_incorrect(module_container: DremioContainer, module_user: InitUser):
    hostname = module_container.get_container_host_ip()
    port = module_container.get_exposed_port(9047)
    dremio = Dremio(hostname=hostname, port=int(port), protocol="http")
    with pytest.raises(AuthError, match="Login incorrect or user not found"):
        dremio.login(username=module_user.userName, password="123456789")
