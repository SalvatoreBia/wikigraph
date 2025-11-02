#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from neo4j import GraphDatabase
from collections import defaultdict
from tqdm import tqdm
import hashlib
import time

# === Config ===
N_SERVERS = 4
NEO4J_SERVERS = {
    0: "bolt://localhost:7687",
    1: "bolt://localhost:7688",
    2: "bolt://localhost:7689",
    3: "bolt://localhost:7690",
}
NEO4J_USER = "neo4j"
NEO4J_PASS = "password"
BATCH_SIZE = 5_000

def get_server_id(title: str) -> int:
    h = hashlib.md5(title.encode('utf-8')).digest()
    return int.from_bytes(h, 'little') % N_SERVERS

class UnionFind:
    def __init__(self):
        self.parent = {}
        self.rank = {}

    def find(self, x):
        if x not in self.parent:
            self.parent[x] = x
            self.rank[x] = 0
            return x
        if self.parent[x] != x:
            self.parent[x] = self.find(self.parent[x])
        return self.parent[x]
    
    def union(self, a, b):
        ra, rb = self.find(a), self.find(b)
        if ra == rb:
            return
        if self.rank[ra] < self.rank[rb]:
            self.parent[ra] = rb
        elif self.rank[ra] > self.rank[rb]:
            self.parent[rb] = ra
        else:
            self.parent[rb] = ra
            self.rank[ra] += 1

def create_drivers():
    drivers = {}
    print("Connessione ai 4 server Neo4j...")
    for i in range(N_SERVERS):
        uri = NEO4J_SERVERS[i]
        drv = GraphDatabase.driver(uri, auth=(NEO4J_USER, NEO4J_PASS))
        drv.verify_connectivity()
        drivers[i] = drv
        print(f"  Server {i} ({uri}) connesso.")
    return drivers

# ===== FASE 1: link cross-shard come coppie (src_title, tgt_title) =====
def find_cross_partition_title_pairs(drivers):
    pairs = []
    print("\nFASE 1: Raccolta coppie titolo per link cross-shard...")
    for server_id in range(N_SERVERS):
        with drivers[server_id].session() as session:
            result = session.run("""
                MATCH (s:Page)-[:LINKS_TO]->(t:Page)
                RETURN s.title AS src, t.title AS tgt
            """)
            for rec in tqdm(result, desc=f"  Lettura link S{server_id}"):
                src, tgt = rec["src"], rec["tgt"]
                if src is None or tgt is None:
                    continue
                if get_server_id(tgt) != server_id:
                    pairs.append((src, tgt))
    print(f"✓ FASE 1: Raccolte {len(pairs):,} coppie cross-shard.")
    return pairs

# ===== (Opzionale, consigliato) FASE 1b: unione intra-shard via communityId locale =====
def find_intra_shard_groups(drivers):
    """
    Restituisce dict per shard: {communityId_locale -> [titles]}.
    Serve ad unire tra loro tutti i titoli che GDS ha messo nella stessa comunità locale.
    """
    groups_per_shard = {}
    print("\nFASE 1b: Raccolta gruppi locali (communityId per shard)...")
    for server_id in range(N_SERVERS):
        groups = defaultdict(list)
        with drivers[server_id].session() as session:
            result = session.run("MATCH (p:Page) RETURN p.title AS title, p.communityId AS cid")
            for rec in tqdm(result, desc=f"  Lettura gruppi S{server_id}"):
                title, cid = rec["title"], rec["cid"]
                if title is None or cid is None or cid == -1:
                    continue
                groups[cid].append(title)
        groups_per_shard[server_id] = groups
        total_titles = sum(len(v) for v in groups.values())
        print(f"  S{server_id}: {len(groups)} gruppi, {total_titles:,} titoli")
    print("✓ FASE 1b: Gruppi locali raccolti.")
    return groups_per_shard

# ===== FASE 2: Union-Find sui TITOLI =====
def build_title_union(pairs, groups_per_shard=None):
    uf = UnionFind()
    # unione da archi cross-shard
    for a, b in tqdm(pairs, desc="Union da edge cross-shard"):
        uf.union(a, b)
    # unione intra-shard per communityId locale (opzionale)
    if groups_per_shard:
        for sid, groups in groups_per_shard.items():
            for cid, titles in tqdm(groups.items(), desc=f"Union intra-shard S{sid}", leave=False):
                if not titles:
                    continue
                base = titles[0]
                for t in titles[1:]:
                    uf.union(base, t)
    # calcolo dei rappresentanti
    rep = {}
    touched = set()
    for a, b in pairs:
        touched.add(a); touched.add(b)
    if groups_per_shard:
        for groups in groups_per_shard.values():
            for lst in groups.values():
                touched.update(lst)
    for t in tqdm(touched, desc="Find rappresentanti"):
        rep[t] = uf.find(t)
    print(f"✓ FASE 2: Create {len(set(rep.values())):,} componenti connesse globali.")
    return rep  # title -> representative_title

# ===== FASE 3: genera globalCommunityId =====
def build_global_ids(rep_by_title):
    """
    Assegna un intero compatto per ogni root.
    Se vuoi stabilità across run, sostituisci con hash del root.
    """
    roots = {}
    next_id = 1
    global_map = {}
    # ordina per root, poi titolo — deterministico
    for title, root in sorted(rep_by_title.items(), key=lambda x: (x[1], x[0])):
        if root not in roots:
            roots[root] = next_id
            next_id += 1
        global_map[title] = roots[root]
    print(f"✓ FASE 3: Assegnati {next_id-1} globalCommunityId.")
    return global_map  # title -> gid

# ===== FASE 4: scrivi globalCommunityId su tutti gli shard =====
def update_global_ids_batch(tx, updates):
    query = """
    UNWIND $updates AS row
    MATCH (p:Page {title: row.title})
    SET p.globalCommunityId = row.gid
    """
    tx.run(query, updates=updates)

def apply_global_map(drivers, global_map):
    print("\nFASE 4: Applicazione globalCommunityId su tutti gli shard...")
    updates = [{"title": t, "gid": gid} for t, gid in global_map.items()]
    if not updates:
        print("Nessun aggiornamento da applicare.")
        return
    for server_id in range(N_SERVERS):
        print(f"Aggiornamento Server {server_id}...")
        with drivers[server_id].session() as session:
            for i in tqdm(range(0, len(updates), BATCH_SIZE), desc=f"  Batch S{server_id}"):
                batch = updates[i:i + BATCH_SIZE]
                session.execute_write(update_global_ids_batch, batch)
        print(f"✓ Server {server_id} aggiornato.")

# ===== Verifica (facoltativa) =====
def verify_cross_shard_consistency(drivers, sample_limit=100_000):
    """
    Controlla che per i link cross-shard valga:
    src.globalCommunityId == tgt.globalCommunityId.
    """
    print("\nVERIFICA: coerenza globalCommunityId sui link cross-shard...")
    inconsistencies = 0
    checked = 0
    for server_id in range(N_SERVERS):
        with drivers[server_id].session() as session:
            result = session.run("""
                MATCH (s:Page)-[:LINKS_TO]->(t:Page)
                RETURN s.title AS src, s.globalCommunityId AS sg,
                       t.title AS tgt, t.globalCommunityId AS tg
            """)
            for rec in tqdm(result, desc=f"  Check S{server_id}"):
                src, sg, tgt, tg = rec["src"], rec["sg"], rec["tgt"], rec["tg"]
                if src is None or tgt is None:
                    continue
                if get_server_id(tgt) == server_id:
                    continue  # non cross
                checked += 1
                if sg is None or tg is None or sg != tg:
                    inconsistencies += 1
                if sample_limit and checked >= sample_limit:
                    break
    if inconsistencies == 0:
        print(f"✓ VERIFICA: nessuna incoerenza trovata su {checked} link cross-shard campionati.")
    else:
        print(f"✗ VERIFICA: trovate {inconsistencies} incoerenze su {checked} link campionati.")
    return inconsistencies

def main():
    start = time.time()
    print("UNIFICAZIONE GLOBALE COMMUNITY (title-based) -> globalCommunityId\n")

    drivers = create_drivers()
    try:
        pairs = find_cross_partition_title_pairs(drivers)
        local_groups = find_intra_shard_groups(drivers)  # opzionale ma consigliato

        rep_by_title = build_title_union(pairs, local_groups)
        global_map = build_global_ids(rep_by_title)
        apply_global_map(drivers, global_map)

        # Verifica veloce (facoltativa)
        verify_cross_shard_consistency(drivers, sample_limit=100_000)

    finally:
        print("\nChiusura connessioni...")
        for d in drivers.values():
            d.close()
    print(f"\nCompletato in {time.time()-start:.2f}s")

if __name__ == "__main__":
    main()
