#!/usr/bin/env python3
"""Stage a bounded Fossil package, upload CAS objects, then conditionally update the head."""
import argparse
import concurrent.futures
import hashlib
import json
import os
import subprocess
from pathlib import Path


HEAD = "chains/0x2105/heads/finalized.bin"
MAX_BYTES = 5_000_000_000  # decimal GB; includes journal and packages in the workspace
CONCURRENCY = 32


def parse_head(raw):
    if len(raw) != 97 or raw[:5] != b"FSEH\x01" or int.from_bytes(raw[13:21], "big") != 8453:
        raise RuntimeError("invalid Base Fossil head")
    return int.from_bytes(raw[5:13], "big"), int.from_bytes(raw[21:29], "big"), raw[61:93]


def workspace_size(root):
    return sum(p.stat().st_size for p in root.rglob("*") if p.is_file())


def enforce_cap(root):
    size = workspace_size(root)
    if size > MAX_BYTES:
        raise RuntimeError(f"STOPPED before R2 publication: {size} local bytes exceeds 5 GB")
    return size

def enforce_remote_cap(current, pending):
    if current > MAX_BYTES or pending > MAX_BYTES - current:
        raise RuntimeError("STOPPED before R2 publication: predicted R2 prefix exceeds 5 GB")


class Publisher:
    def __init__(self, workspace, fossil, bucket, prefix):
        import boto3
        from botocore.config import Config
        self.workspace = workspace
        self.mirror = workspace / "mirror"
        self.fossil = fossil
        self.bucket = bucket
        self.prefix = prefix.rstrip("/") + "/"
        self.s3 = boto3.client(
            "s3", endpoint_url=os.environ["CF_S3_API_ENDPOINT"], region_name="auto",
            aws_access_key_id=os.environ["CF_ACCESS_KEY_ID"],
            aws_secret_access_key=os.environ["CF_SECRET_ACCESS_KEY"],
            config=Config(signature_version="s3v4", max_pool_connections=CONCURRENCY + 2,
                          retries={"max_attempts": 5}),
        )
        self.remote_keys = set()
        self.remote_bytes = 0
        for page in self.s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=self.prefix):
            for obj in page.get("Contents", []):
                self.remote_keys.add(obj["Key"][len(self.prefix):])
                self.remote_bytes += obj["Size"]
        enforce_remote_cap(self.remote_bytes, 0)

    def remote_head(self):
        response = self.s3.get_object(Bucket=self.bucket, Key=self.prefix + HEAD)
        raw = response["Body"].read(98)
        parse_head(raw)
        return raw, response["ETag"]

    def put_immutable(self, relative):
        from botocore.exceptions import ClientError
        raw = (self.mirror / relative).read_bytes()
        digest = hashlib.sha256(raw).hexdigest()
        if relative != f"objects/sha256/{digest[:2]}/{digest}":
            raise RuntimeError("local immutable object has wrong digest/path")
        key = self.prefix + relative
        try:
            self.s3.put_object(Bucket=self.bucket, Key=key, Body=raw, IfNoneMatch="*")
        except ClientError as error:
            if error.response.get("Error", {}).get("Code") not in ("PreconditionFailed", "412"):
                raise
            found = self.s3.get_object(Bucket=self.bucket, Key=key)["Body"].read(len(raw) + 1)
            if found != raw:
                raise RuntimeError("R2 immutable object collision") from error
        return relative

    def publish(self, package):
        head_path = self.mirror / HEAD
        prior_remote, prior_etag = self.remote_head()
        if head_path.read_bytes() != prior_remote:
            raise RuntimeError("local mirror is not at R2 head; reconcile before retry")
        generation, number, previous_commit = parse_head(prior_remote)
        records = package.read_text().splitlines()
        first = int(json.loads(records[0])["preceding_number"], 16) + 1
        trailer = json.loads(records[-1])
        last, hash_value = int(trailer["end_number"], 16), trailer["end_hash"]
        if first != number + 1 or last < first or last - first >= 1000:
            raise RuntimeError("package is not a bounded contiguous successor")
        subprocess.run([str(self.fossil), "archive", "--input", str(package),
                        "--store", str(self.mirror), "--chain-id", "0x2105",
                        "--gate", "finalized", "--finalized-head", f"{last}:{hash_value}"],
                       check=True, timeout=600)
        total = enforce_cap(self.workspace)
        next_head = head_path.read_bytes()
        new_generation, new_number, commit_hash = parse_head(next_head)
        if new_generation != generation + 1 or new_number != last:
            raise RuntimeError("unexpected staged publication head")
        commit_path = self.mirror / f"objects/sha256/{commit_hash.hex()[:2]}/{commit_hash.hex()}"
        commit = commit_path.read_bytes()
        if len(commit) != 314 or hashlib.sha256(commit).digest() != commit_hash or commit[165:197] != previous_commit:
            raise RuntimeError("staged commit does not extend R2 head")
        missing = [str(path.relative_to(self.mirror))
                   for path in (self.mirror / "objects/sha256").glob("*/*")
                   if str(path.relative_to(self.mirror)) not in self.remote_keys]
        new_bytes = sum((self.mirror / relative).stat().st_size for relative in missing)
        enforce_remote_cap(self.remote_bytes, new_bytes)
        with concurrent.futures.ThreadPoolExecutor(max_workers=CONCURRENCY) as executor:
            for relative in executor.map(self.put_immutable, missing):
                self.remote_keys.add(relative)
        self.remote_bytes += new_bytes
        current, etag = self.remote_head()
        if current != prior_remote or etag != prior_etag:
            raise RuntimeError("R2 head advanced concurrently; refusing to overwrite")
        self.s3.put_object(Bucket=self.bucket, Key=self.prefix + HEAD, Body=next_head,
                           ContentType="application/octet-stream", IfMatch=prior_etag)
        if self.remote_head()[0] != next_head:
            raise RuntimeError("R2 head does not match staged publication")
        print(f"PUBLISHED {first}..{last} generation={new_generation} objects={len(missing)} bytes={total}", flush=True)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("package", type=Path)
    p.add_argument("--workspace", required=True, type=Path)
    p.add_argument("--fossil", required=True, type=Path)
    p.add_argument("--bucket", required=True)
    p.add_argument("--prefix", default="v1/")
    args = p.parse_args()
    Publisher(args.workspace, args.fossil, args.bucket, args.prefix).publish(args.package)


if __name__ == "__main__":
    main()
