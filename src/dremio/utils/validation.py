import re
from typing import Callable, Dict, Literal, Any, Union


class ValidationError(Exception):
    pass


class Validation:
    regex = {
        "email": r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$",
        "password": r"^.{8,}$",
        "username": r"^[a-zA-Z0-9_.-@]{3,}$",
        "hostname": r"^https?:\/\/(?:www\.)?[-a-zA-Z0-9@:%._\+~#=]{1,256}$",
        "port": r"^[0-9]{1,5}$",
    }

    def __val(self, value, regex, error):
        # if not re.match(regex, value):
        #  raise ValidationError(error)
        return value

    def __call__(self, **kwargs: str):
        """Check if the values of the kwargs match the regex.

        Returns:
            str: if only one key is passed
            dict[str,str]: if more than one key is passed
        """
        for target, value in kwargs.items():
            self.__val(value, self.regex[target], f"{target} is invalid")
        if len(kwargs) == 1:
            return list(kwargs.values())[0]
        return kwargs

    def hostname(
        self,
        hostname: str,
        port: Union[int, None] = None,
        protocol: Union[Literal["https", "http"], None] = None,
    ) -> str:
        """Check if the hostname is valid and return it."""
        has_protocol: Callable[[str], bool] = lambda x: bool(
            re.findall(r"^(http|https):\/\/", x)
        )
        has_port: Callable[[str], bool] = lambda x: bool(re.findall(r":[0-9]{1,4}", x))

        if has_protocol(hostname):
            if protocol:
                hostname = protocol + "://" + hostname.split("://")[1]
        else:
            if protocol:
                hostname = protocol + "://" + hostname
            else:
                hostname = "https://" + hostname

        try:
            protocol = hostname.split("://")[0]  # type: ignore
        except:
            protocol = "https"

        hostname = hostname if not hostname.endswith("/") else hostname[:-1]

        if not has_port(hostname):
            if not port:
                port = 80 if protocol == "http" else 443
            if (protocol == "https" and port != 443) or (
                protocol == "http" and port != 80
            ):
                hn = hostname.split("/")[2].split(":")[0]
                hostname = hostname.replace(hn, hn + f":{port}")
        return self.__val(hostname, self.regex["hostname"], "hostname is invalid")
