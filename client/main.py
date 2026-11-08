import argparse
import hashlib
import json
import sys
from pathlib import Path

import httpx

DEFAULT_URL = "http://localhost:8000"


def upload(base_url: str, filename: str):
    path = Path(filename)

    if not path.is_file():
        raise SystemExit(f"File not found: {path}")

    with path.open("rb") as file:
        response = httpx.post(
            f"{base_url}/v1/files",
            files={
                "file": (
                    path.name,
                    file,
                    "application/octet-stream",
                )
            },
            timeout=3600,
        )

    response.raise_for_status()
    result = response.json()

    print(json.dumps(result, indent=2))
    print(f"\nFile ID: {result['file_id']}")


def download(base_url: str, file_id: str, destination: str):
    output = Path(destination)

    with httpx.stream(
        "GET",
        f"{base_url}/v1/files/{file_id}/download",
        timeout=3600,
    ) as response:
        response.raise_for_status()

        digest = hashlib.sha256()
        size = 0

        with output.open("wb") as file:
            for block in response.iter_bytes():
                file.write(block)
                digest.update(block)
                size += len(block)

    print(f"Downloaded: {output}")
    print(f"Size: {size} bytes")
    print(f"SHA-256: {digest.hexdigest()}")


def main():
    parser = argparse.ArgumentParser(
        description="Distributed content-addressed storage client"
    )
    parser.add_argument("--url", default=DEFAULT_URL)

    subparsers = parser.add_subparsers(dest="command", required=True)

    upload_parser = subparsers.add_parser("upload")
    upload_parser.add_argument("file")

    download_parser = subparsers.add_parser("download")
    download_parser.add_argument("file_id")
    download_parser.add_argument("output")

    delete_parser = subparsers.add_parser("delete")
    delete_parser.add_argument("file_id")

    info_parser = subparsers.add_parser("info")
    info_parser.add_argument("file_id")

    subparsers.add_parser("nodes")

    args = parser.parse_args()
    base_url = args.url.rstrip("/")

    try:
        if args.command == "upload":
            upload(base_url, args.file)
        elif args.command == "download":
            download(base_url, args.file_id, args.output)
        elif args.command == "delete":
            response = httpx.delete(f"{base_url}/v1/files/{args.file_id}", timeout=60)
            response.raise_for_status()
            print(json.dumps(response.json(), indent=2))
        elif args.command == "info":
            response = httpx.get(f"{base_url}/v1/files/{args.file_id}", timeout=10)
            response.raise_for_status()
            print(json.dumps(response.json(), indent=2))
        elif args.command == "nodes":
            response = httpx.get(f"{base_url}/v1/nodes", timeout=10)
            response.raise_for_status()
            print(json.dumps(response.json(), indent=2))
    except httpx.HTTPError as exc:
        print(f"Request failed: {exc}", file=sys.stderr)
        raise SystemExit(1)


if __name__ == "__main__":
    main()