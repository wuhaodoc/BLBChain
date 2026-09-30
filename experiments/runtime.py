"""Run isolated, opt-in PBFT overhead trials. Python 3.8+, standard library only."""
import argparse
import csv
import hashlib
import json
import os
from pathlib import Path
import platform
import random
import socket
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "experiments"))
VARIANTS = {"A": (False, False), "B": (True, False), "C": (False, True), "D": (True, True)}


def sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def synthetic(path, total, shards, seed):
    """Deterministic synthetic smoke workload, NOT paper evaluation data."""
    rng = random.Random(seed)
    accounts = [f"{i:040x}" for i in range(1, 129)]
    hot = [a for a in accounts if int(a, 16) % shards == 0][:8]
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["index", "unused", "sender", "recipient", "value"])
        for i in range(total):
            sender = rng.choice(hot if rng.random() < 0.8 else accounts)
            recipient = rng.choice(accounts)
            while recipient == sender:
                recipient = rng.choice(accounts)
            w.writerow([i, "", sender, recipient, 1])


def prepare_dataset(source, target, total, sender, recipient, value, has_header=True):
    with open(source, newline="", encoding="utf-8-sig") as src, open(target, "w", newline="", encoding="utf-8") as dst:
        r, w = csv.reader(src), csv.writer(dst)
        if has_header:
            next(r, None)
        w.writerow(["index", "unused", "sender", "recipient", "value"])
        for i in range(total):
            row = next(r, None)
            if row is None:
                raise ValueError(f"Dataset has fewer than {total} rows")
            a, b = row[sender].strip().lower(), row[recipient].strip().lower()
            a = a[2:] if a.startswith("0x") else a
            b = b[2:] if b.startswith("0x") else b
            if len(a) != 40 or len(b) != 40:
                raise ValueError(f"Invalid address at input row {i + (2 if has_header else 1)}; preprocess/filter the input first")
            int(a, 16), int(b, 16)
            amount = int(row[value])
            if amount < 0:
                raise ValueError("Negative value")
            w.writerow([i, "", a, b, amount])


def address_table(shards, nodes):
    # Ask the OS for available ports. Close reservations immediately before launch.
    reservations, table = [], {}
    try:
        for s in list(range(shards)) + [2147483647]:
            table[str(s)] = {}
            for n in range(nodes if s != 2147483647 else 1):
                sock = socket.socket()
                sock.bind(("127.0.0.1", 0))
                reservations.append(sock)
                table[str(s)][str(n)] = f"127.0.0.1:{sock.getsockname()[1]}"
        return table
    finally:
        for sock in reservations:
            sock.close()


def write_json(path, data):
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")


def run_trial(binary, folder, config, shards, nodes, timeout):
    folder.mkdir()
    write_json(folder / "paramsConfig.json", config)
    table = address_table(shards, nodes)
    write_json(folder / "ipTable.json", table)
    processes, logs = [], []
    deadline = time.monotonic() + timeout + 30
    try:
        def launch(name, args):
            log = open(folder / f"{name}.log", "w", encoding="utf-8")
            logs.append(log)
            flags = subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0
            p = subprocess.Popen([str(binary), *args], cwd=folder, stdout=log, stderr=subprocess.STDOUT, creationflags=flags)
            processes.append(p)
            return p

        for s in range(shards):
            for n in range(nodes):
                launch(f"s{s}_n{n}", ["-s", str(s), "-n", str(n), "-S", str(shards), "-N", str(nodes)])
        # Readiness checks never start a second instance or kill unrelated processes.
        for s in range(shards):
            for n in range(nodes):
                port = int(table[str(s)][str(n)].split(":")[1])
                while True:
                    if any(p.poll() is not None for p in processes):
                        raise RuntimeError("Node exited during startup; inspect node logs")
                    try:
                        with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                            break
                    except OSError:
                        if time.monotonic() > deadline:
                            raise TimeoutError("Node startup timed out")
                        time.sleep(0.1)
        supervisor = launch("supervisor", ["-c", "-S", str(shards), "-N", str(nodes)])
        while supervisor.poll() is None:
            if any(p.poll() is not None and p.returncode != 0 for p in processes):
                raise RuntimeError("Process failed; inspect logs")
            if time.monotonic() > deadline:
                raise TimeoutError("Trial timed out; partial output is not a valid result")
            time.sleep(0.2)
        if supervisor.returncode != 0:
            raise RuntimeError("Supervisor failed; inspect supervisor.log")
        for p in processes:
            p.wait(timeout=max(1, deadline - time.monotonic()))
            if p.returncode != 0:
                raise RuntimeError("Node exited with nonzero status")
        return {"status": "complete"}
    except Exception as exc:
        return {"status": "failed", "error": str(exc)}
    finally:
        for p in processes:
            if p.poll() is None:
                p.terminate()
        for p in processes:
            try:
                p.wait(timeout=5)
            except subprocess.TimeoutExpired:
                p.kill()
                p.wait()
        for log in logs:
            log.close()

