__all__ = ["_MixinReflection"]  # this is like `export ...` in typescript

import logging
from typing import Any
import requests
from time import sleep

from ..utils.converter import to_dict
from ..models import *

from . import BaseClass
from ._catalog import _MixinCatalog


class _MixinReflection(_MixinCatalog, BaseClass):

    def get_reflections(self) -> list[Reflection]:
        """Get all reflections.

        Returns:
          list[Reflection]: reflections.
        """
        url = f"{self.hostname}/api/v3/reflection"
        response = requests.get(url, headers=self._headers)
        self._raise_error(response)
        try:
            data = response.json()["data"]
        except KeyError:
            raise KeyError("Expected data-tag in reflections response, but not found")
        return [cast(Reflection, reflection) for reflection in data]

    def get_reflection(self, reflection_id: Union[UUID, str]) -> Reflection:
        """Get one reflection.

        Parameters:
          id: reflection uuid.

        Returns:
          Reflection: reflection object.
        """
        url = f"{self.hostname}/api/v3/reflection/{reflection_id}"
        response = requests.get(url, headers=self._headers)
        self._raise_error(response)
        return cast(Reflection, response.json())

    def create_reflection(
        self, dataset_id: str | UUID, name: str, reflection: NewReflection
    ) -> Reflection:
        url = f"{self.hostname}/api/v3/reflection"
        payload = to_dict(reflection) | {
            "datasetId": dataset_id,
            "name": name,
        }
        response = requests.post(url, headers=self._headers, json=payload)
        self._raise_error(response)
        return cast(Reflection, response.json())

    def update_reflection(self, reflection: Reflection) -> Reflection:
        """Update the reflection.

        Parameters:
          reflection: reflection object.

        Returns:
          Reflection: updated reflection.
        """
        url = f"{self.hostname}/api/v3/reflection/{reflection.id}"
        payload: dict[str, Any] = to_dict(reflection)
        response = requests.put(url, headers=self._headers, json=payload)
        self._raise_error(response)
        return cast(Reflection, response.json())

    def delete_reflection(self, reflection_id: UUID | str) -> None:
        url = f"{self.hostname}/api/v3/reflection/{reflection_id}"
        response = requests.delete(url, headers=self._headers)
        self._raise_error(response)
        if response.status_code not in [200, 204]:
            raise DremioConnectorError(
                "Deletion of reflection failed",
                f"Failed to delete reflection {reflection_id}",
            )
        return None

    def refresh_reflection(
        self, reflection_id: UUID | str, verbose: bool = False
    ) -> Reflection:
        """Update the reflection.

        Parameters:
          reflection_id: reflection uuid.
          verbose: Write infos and warnings into the default logger. Default: False.

        Returns:
          Reflection: refreshed reflection.
        """
        if verbose:
            logging.info(f"Updating reflection {reflection_id}")
        # load reflection to disable and reenable it
        reflection = self.get_reflection(reflection_id)
        reflection.enabled = False
        refreshing_reflection = self.update_reflection(reflection)
        del reflection
        sleep(1)
        refreshing_reflection.enabled = True
        _ = self.update_reflection(refreshing_reflection)
        del refreshing_reflection
        sleep(1)

        # retrying
        retried = False
        while True:
            refreshed_reflection = self.get_reflection(reflection_id)
            status = refreshed_reflection.status.combinedStatus
            if status == "checkup_reflection":
                if verbose:
                    logging.info(
                        f"SUCCESS: {refreshed_reflection.name} - Reflection was refreshed"
                    )
                break
            elif status == "REFRESHING":
                sleep(1)
                continue
            else:
                if status in {"DISABLED", "INVALID", "CANNOT_ACCELERATE_MANUAL"}:
                    if verbose:
                        logging.warn(
                            f"{refreshed_reflection.name} - Reflection refresh cannot be completed. Reflection status: {status}"
                        )
                elif status in {
                    "FAILED",
                    "INCOMPLETE",
                    "EXPIRED",
                    "CAN_ACCELERATE_WITH_FAILURES",
                    "CANNOT_ACCELERATE_SCHEDULED",
                }:
                    if verbose:
                        logging.warning(
                            (
                                f"{refreshed_reflection.name} - Reflection refresh was not successful. Reflection status: {status}"
                            )
                        )
                    if not retried:
                        sleep(5)
                        if verbose:
                            logging.info("Retrying...")
                        retried = True
                        continue
                break
        return refreshed_reflection
