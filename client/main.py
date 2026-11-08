import argparse
import hashlib
import json
import sys
from pathlib import Path

import httpx

DEFAULT_URL = "http://localhost:8000"


def upload(base_url: str, filename: str, bucket: str = "default",
           key: str | None = None, chunking: str = "fixed"):
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
            data={"bucket": bucket, "key": key or path.name,
                  "chunking": chunking},
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
    upload_parser.add_argument("--bucket", default="default")
    upload_parser.add_argument("--key")
    upload_parser.add_argument("--chunking", choices=["fixed", "cdc"],
                               default="fixed")

    download_parser = subparsers.add_parser("download")
    download_parser.add_argument("file_id")
    download_parser.add_argument("output")

    delete_parser = subparsers.add_parser("delete")
    delete_parser.add_argument("file_id")

    info_parser = subparsers.add_parser("info")
    info_parser.add_argument("file_id")

    subparsers.add_parser("nodes")
    subparsers.add_parser("ring")
    subparsers.add_parser("stats")
    subparsers.add_parser("status")
    subparsers.add_parser("repair-status")
    subparsers.add_parser("buckets")

    bucket_parser = subparsers.add_parser("bucket-create")
    bucket_parser.add_argument("name")
    bucket_parser.add_argument("--redundancy", choices=["replication", "ec"],
                               default="replication")
    bucket_parser.add_argument("--k", type=int, default=4)
    bucket_parser.add_argument("--m", type=int, default=2)

    bstats_parser = subparsers.add_parser("bucket-stats")
    bstats_parser.add_argument("name")

    args = parser.parse_args()
    base_url = args.url.rstrip("/")

    try:
        if args.command == "upload":
            upload(base_url, args.file, args.bucket, args.key, args.chunking)
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
        elif args.command in ("ring", "stats", "status", "repair-status",
                              "buckets", "bucket-stats"):
            path = {"status": "cluster/status", "repair-status": "repair/status",
                    "bucket-stats": f"buckets/{getattr(args, 'name', '')}/stats"
                    }.get(args.command, args.command)
            response = httpx.get(f"{base_url}/v1/{path}", timeout=60)
            response.raise_for_status()
            print(json.dumps(response.json(), indent=2))
        elif args.command == "bucket-create":
            response = httpx.post(
                f"{base_url}/v1/buckets", timeout=30,
                json={"name": args.name, "redundancy": args.redundancy,
                      "k": args.k, "m": args.m})
            if response.status_code >= 400:
                print(response.text, file=sys.stderr)
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