from dataclasses import dataclass
from typing_extensions import Self
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs
from src.dremio import Dremio, FlightConfig


@dataclass(kw_only=True)
class InitUser:
    userName: str = "admin"
    firstName: str = "admin"
    lastName: str = "admin"
    email: str = "admin@admin.de"
    password: str = "admin_password_123"


class DremioContainer(DockerContainer):
    image = "dremio/dremio-oss"

    def __init__(
        self,
        init_user: InitUser = InitUser(),
        bind_ports: dict[int, int | None] = {
            9047: None,
            31010: None,
            32010: None,
            45678: None,
        },
        docker_client_kw: dict | None = None,
        **kwargs,
    ):
        super().__init__(self.image, docker_client_kw, **kwargs)
        for container_port, host_port in bind_ports.items():
            self.with_bind_ports(container_port, host_port)
        self.init_user: InitUser = init_user

    def start(self, auto_user_creation: bool = True) -> Self:
        self = super().start()
        delay = wait_for_logs(self, r"^.*Dremio Daemon Started as master.*$")
        ip = self.get_container_host_ip()
        port = self.get_exposed_port(9047)
        print(f"Container is ready after {delay} seconds")
        print(f"Connect to Dremio at {ip}:{port}")
        if auto_user_creation:
            self.create_firstuser()
        return self

    def create_firstuser(self, user: InitUser | None = None) -> InitUser:
        if not user:
            user = self.init_user
        self.exec(
            f"""\
              curl 'http://localhost:9047/apiv2/bootstrap/firstuser' -X PUT \
              -H 'Authorization: _dremionull' -H 'Content-Type: application/json' \
              --data-binary '{{"userName":"{user.userName}","firstName":"{user.firstName}","lastName":"{user.lastName}","email":"{user.email}","createdAt":1526186430755,"password":"{user.password}"}}'\
            """
        )
        print(f"InitUser {user.userName} created")
        return user

    def add_sample_sources(self, *sample_files_to_format: str) -> list[str]:
        "returns a list of paths for the created sample datasets"
        print("import samples")
        user = self.dremio.login(self.init_user.userName, self.init_user.password)
        res = self.exec(
            f"""
              curl 'http://localhost:9047/apiv2/source/Samples' -X PUT \
              -H 'Authorization: _dremio{user.token}' -H 'Content-Type: application/json' \
              --data-binary '{{"config":{{"externalBucketList":["samples.dremio.com"],"credentialType":"NONE","secure":false,"propertyList":[]}},"name":"Samples","accelerationRefreshPeriod":3600000,"accelerationGracePeriod":10800000,"accelerationNeverRefresh":true,"accelerationNeverExpire":true,"accelerationActivePolicyType":"PERIOD","accelerationRefreshSchedule":"0 0 8 * * *","type":"S3"}}'
            """
        )
        for sample in sample_files_to_format:
            print("formating", sample)
            res = self.exec(
                f"""
                  curl 'http://localhost:9047/apiv2/source/Samples/file_format/samples.dremio.com/{sample}' -X PUT \
                  -H 'Authorization: _dremio{user.token}' -H 'Content-Type: application/json' \
                  --data-binary '{{"fieldDelimiter":",","quote":"\\"","comment":"#","lineDelimiter":"\\r\\n","escape":"\\"","extractHeader":true,"trimHeader":true,"skipFirstLine":false,"type":"Text"}}'
                """
            )
        return [
            f'Samples."samples.dremio.com"."{sample}"'
            for sample in sample_files_to_format
        ]

    @property
    def dremio(self) -> Dremio:
        hostname = self.get_container_host_ip()
        port = self.get_exposed_port(9047)
        grpc_port = self.get_exposed_port(32010)
        return Dremio(
            hostname=hostname,
            port=int(port),
            protocol="http",
            flight_config=FlightConfig(port=int(grpc_port)),
            username=self.init_user.userName,
            password=self.init_user.password,
        )

    def __enter__(self) -> tuple[Self, Dremio]:
        return self, self.dremio


if __name__ == "__main__":
    from time import sleep

    with DremioContainer() as container:
        sleep(300)
