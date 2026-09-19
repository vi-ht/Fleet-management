"""Minimal WebHDFS client used by the batch bootstrap."""

import os
import time
from pathlib import Path
from urllib.parse import quote

import requests


BASE_URL = os.getenv("HDFS_WEB_URL", "http://namenode:9870").rstrip("/")
USER = os.getenv("HDFS_USER", "root")


def _url(path: str) -> str:
    clean = "/" + path.lstrip("/")
    return f"{BASE_URL}/webhdfs/v1{quote(clean, safe='/')}"


def wait_ready(timeout: int = 180) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            response = requests.get(
                f"{BASE_URL}/jmx",
                params={"qry": "Hadoop:service=NameNode,name=NameNodeStatus"},
                timeout=5,
            )
            state_response = requests.get(
                f"{BASE_URL}/jmx",
                params={"qry": "Hadoop:service=NameNode,name=FSNamesystemState"},
                timeout=5,
            )
            if response.ok and state_response.ok:
                beans = state_response.json().get("beans", [])
                if beans and beans[0].get("FSState") == "Operational":
                    return
        except requests.RequestException:
            pass
        except (ValueError, KeyError, TypeError):
            pass
        time.sleep(2)
    raise TimeoutError("HDFS NameNode did not become operational")


def mkdirs(path: str) -> None:
    response = requests.put(
        _url(path), params={"op": "MKDIRS", "user.name": USER}, timeout=30
    )
    response.raise_for_status()


def set_permission(path: str, permission: str = "777") -> None:
    response = requests.put(
        _url(path),
        params={"op": "SETPERMISSION", "permission": permission, "user.name": USER},
        timeout=30,
    )
    response.raise_for_status()


def put_file(local_path: str, remote_path: str) -> None:
    response = requests.put(
        _url(remote_path),
        params={"op": "CREATE", "overwrite": "true", "user.name": USER},
        allow_redirects=False,
        timeout=30,
    )
    if response.status_code not in (307, 201):
        response.raise_for_status()
    location = response.headers.get("Location", response.url)
    with open(local_path, "rb") as source:
        upload = requests.put(location, data=source, timeout=120)
    upload.raise_for_status()


def exists(remote_path: str) -> bool:
    response = requests.get(
        _url(remote_path), params={"op": "GETFILESTATUS", "user.name": USER}, timeout=30
    )
    return response.ok
