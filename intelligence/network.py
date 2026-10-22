from collections import Counter, defaultdict
from datetime import timezone

import networkx as nx
from sklearn.feature_extraction.text import TfidfVectorizer

from intelligence.audience import _event_text as event_text
from intelligence.audience import _parse_time as parse_time
from intelligence.audience import _sentiment_shares, tokenize

NETWORK_PLATFORMS = ("youtube", "telegram")
MIN_NETWORK_EVENTS = 20
MIN_COMMUNITY_MESSAGES = 5
MIN_TOPIC_MENTIONS = 3
MIN_NODE_SENTIMENT_EVENTS = 3
MIN_BRIDGE_SHARE = 0.2
TOP_NODES = 10
TOP_TERMS = 5
TOP_SOURCES = 3
TOP_TOPICS = 3
BETWEENNESS_SAMPLE_THRESHOLD = 1500
BETWEENNESS_SAMPLE_SIZE = 400
RANDOM_STATE = 42


def _identity(tokens):
    return tokens


def _to_utc(value):
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _author_label(event):
    name = event.get("author_name")
    if name:
        return str(name)
    return f"User {event['author_id']}"


def _source_label(event):
    name = event.get("source_name")
    source_id = event.get("source_id")
    if event["platform"] == "youtube":
        return f"{name or 'YouTube'} · {source_id}"
    return str(name or source_id)


def is_network_event(event):
    if event.get("platform") not in NETWORK_PLATFORMS:
        return False
    author_id = event.get("author_id")
    source_id = event.get("source_id")
    if not author_id or not source_id or source_id == "unknown":
        return False
    return True


def build_graph(events):
    graph = nx.Graph()
    records = []
    for event in events:
        if not is_network_event(event):
            continue
        author_id = event.get("author_id")
        source_id = event.get("source_id")
        platform = event["platform"]
        author_node = f"author::{platform}::{author_id}"
        source_node = f"source::{platform}::{source_id}"
        if not graph.has_node(author_node):
            graph.add_node(author_node, type="author", label=_author_label(event), platform=platform, messages=0)
        if not graph.has_node(source_node):
            graph.add_node(source_node, type="source", label=_source_label(event), platform=platform, messages=0)
        graph.nodes[author_node]["messages"] += 1
        graph.nodes[source_node]["messages"] += 1
        if graph.has_edge(author_node, source_node):
            graph[author_node][source_node]["weight"] += 1
        else:
            graph.add_edge(author_node, source_node, weight=1)
        records.append({"event": event, "author_node": author_node, "source_node": source_node})
    return graph, records


def _sentiment_score(events):
    scores = []
    for event in events:
        analysis = event.get("analysis")
        if not analysis:
            continue
        label = analysis["sentiment"]["label"]
        scores.append(1 if label == "Positive" else -1 if label == "Negative" else 0)
    if len(scores) < MIN_NODE_SENTIMENT_EVENTS:
        return None
    return round(sum(scores) / len(scores), 3)


def _centrality(graph):
    degree = nx.degree_centrality(graph)
    try:
        pagerank = nx.pagerank(graph, weight="weight")
    except nx.PowerIterationFailedConvergence:
        pagerank = dict(degree)
    if graph.number_of_nodes() > BETWEENNESS_SAMPLE_THRESHOLD:
        betweenness = nx.betweenness_centrality(graph, k=BETWEENNESS_SAMPLE_SIZE, seed=RANDOM_STATE)
    else:
        betweenness = nx.betweenness_centrality(graph)
    return {"degree": degree, "pagerank": pagerank, "betweenness": betweenness}


def _community_terms(token_lists_by_community):
    ids = list(token_lists_by_community)
    if not ids:
        return {}
    documents = [token_lists_by_community[cid] for cid in ids]
    terms = {}
    if len(ids) >= 2:
        try:
            vectorizer = TfidfVectorizer(analyzer=_identity, sublinear_tf=True)
            matrix = vectorizer.fit_transform(documents)
            names = vectorizer.get_feature_names_out()
            for row, cid in enumerate(ids):
                dense = matrix[row].toarray().ravel()
                order = dense.argsort()[::-1][:TOP_TERMS]
                terms[cid] = [str(names[i]) for i in order if dense[i] > 0]
            return terms
        except ValueError:
            pass
    for cid, tokens in zip(ids, documents):
        terms[cid] = [word for word, _ in Counter(tokens).most_common(TOP_TERMS)]
    return terms


def _node_info(graph, node, metrics, node_community, node_events):
    data = graph.nodes[node]
    return {
        "node_id": node,
        "label": data["label"],
        "type": data["type"],
        "platform": data["platform"],
        "messages": data["messages"],
        "connections": graph.degree(node),
        "pagerank": round(metrics["pagerank"][node], 5),
        "degree_centrality": round(metrics["degree"][node], 4),
        "betweenness": round(metrics["betweenness"][node], 4),
        "community_id": node_community.get(node),
        "sentiment_score": _sentiment_score(node_events.get(node, [])),
    }


def _spread_label(reached):
    if reached >= 3:
        return "high"
    if reached == 2:
        return "medium"
    return "contained"


def _propagation(topics, records, record_community, significant_ids):
    topic_of_event = {}
    topic_names = {}
    for topic in topics:
        topic_names[topic["topic_id"]] = topic["name"]
        for event_id in topic.get("event_ids", []):
            topic_of_event[event_id] = topic["topic_id"]

    times = defaultdict(lambda: defaultdict(list))
    for record, community in zip(records, record_community):
        if community not in significant_ids:
            continue
        topic_id = topic_of_event.get(record["event"]["event_id"])
        if topic_id is None:
            continue
        moment = _to_utc(parse_time(record["event"].get("timestamp")))
        times[topic_id][community].append(moment)

    propagation = []
    for topic_id, by_community in times.items():
        reached = {cid: moments for cid, moments in by_community.items() if len(moments) >= MIN_TOPIC_MENTIONS}
        if not reached:
            continue
        timeline = []
        for cid, moments in reached.items():
            valid = [m for m in moments if m is not None]
            first = min(valid) if valid else None
            timeline.append({"community_id": cid, "mentions": len(moments), "first_seen": first})
        timeline.sort(key=lambda item: (item["first_seen"] is None, item["first_seen"]))
        origin_time = timeline[0]["first_seen"]
        for item in timeline:
            first = item["first_seen"]
            item["lag_hours"] = round((first - origin_time).total_seconds() / 3600, 1) if first and origin_time else None
            item["first_seen"] = first.replace(microsecond=0).isoformat() if first else None
        propagation.append({
            "topic_id": topic_id,
            "name": topic_names[topic_id],
            "communities_reached": len(reached),
            "total_communities": len(significant_ids),
            "spread": _spread_label(len(reached)),
            "origin_community": timeline[0]["community_id"],
            "total_mentions": sum(item["mentions"] for item in timeline),
            "timeline": timeline,
        })
    propagation.sort(key=lambda p: (-p["communities_reached"], -p["total_mentions"]))
    return propagation


def analyze_network(events, topics=None):
    result = {
        "status": "insufficient_data",
        "method": "louvain+pagerank+betweenness",
        "total_events": len(events),
        "network_events": 0,
        "excluded_events": len(events),
        "min_required": MIN_NETWORK_EVENTS,
        "communities": [],
        "top_authors": [],
        "top_sources": [],
        "bridge_nodes": [],
        "propagation": [],
        "limitations": [],
    }

    graph, records = build_graph(events)
    result["network_events"] = len(records)
    result["excluded_events"] = len(events) - len(records)
    if len(records) < MIN_NETWORK_EVENTS or graph.number_of_edges() == 0:
        return result, graph

    partition = nx.community.louvain_communities(graph, weight="weight", seed=RANDOM_STATE)
    node_part = {}
    for index, nodes in enumerate(partition):
        for node in nodes:
            node_part[node] = index

    part_records = defaultdict(list)
    for record in records:
        part_records[node_part[record["author_node"]]].append(record)

    significant_parts = sorted(
        (index for index, items in part_records.items() if len(items) >= MIN_COMMUNITY_MESSAGES),
        key=lambda index: len(part_records[index]),
        reverse=True,
    )
    community_id_of_part = {index: f"community_{position:02d}" for position, index in enumerate(significant_parts, start=1)}
    significant_ids = set(community_id_of_part.values())

    node_community = {node: community_id_of_part.get(index) for node, index in node_part.items()}
    record_community = [node_community[record["author_node"]] for record in records]

    node_events = defaultdict(list)
    for record in records:
        node_events[record["author_node"]].append(record["event"])
        node_events[record["source_node"]].append(record["event"])

    metrics = _centrality(graph)

    topic_of_event = {}
    topic_names = {}
    for topic in topics or []:
        topic_names[topic["topic_id"]] = topic["name"]
        for event_id in topic.get("event_ids", []):
            topic_of_event[event_id] = topic["topic_id"]

    tokens_by_community = {}
    for index in significant_parts:
        cid = community_id_of_part[index]
        collected = []
        for record in part_records[index]:
            collected.extend(tokenize(event_text(record["event"])))
        tokens_by_community[cid] = collected
    terms_by_community = _community_terms(tokens_by_community)

    communities = []
    for index in significant_parts:
        cid = community_id_of_part[index]
        items = part_records[index]
        member_events = [item["event"] for item in items]
        nodes = partition[index]
        authors = [n for n in nodes if graph.nodes[n]["type"] == "author"]
        sources = [n for n in nodes if graph.nodes[n]["type"] == "source"]

        platform_counts = Counter(event["platform"] for event in member_events)
        source_counts = Counter(item["source_node"] for item in items)
        topic_counts = Counter(
            topic_of_event[event["event_id"]] for event in member_events if event["event_id"] in topic_of_event
        )

        communities.append({
            "community_id": cid,
            "nodes": len(nodes),
            "authors": len(authors),
            "sources": len(sources),
            "messages": len(items),
            "platforms": {p: round(c / len(items), 3) for p, c in platform_counts.most_common()},
            "sentiment": _sentiment_shares(member_events),
            "top_terms": terms_by_community.get(cid, []),
            "top_topics": [
                {"topic_id": tid, "name": topic_names[tid], "count": count}
                for tid, count in topic_counts.most_common(TOP_TOPICS)
            ],
            "top_sources": [
                {"label": graph.nodes[node]["label"], "messages": count}
                for node, count in source_counts.most_common(TOP_SOURCES)
            ],
        })

    authors_sorted = sorted(
        (n for n, d in graph.nodes(data=True) if d["type"] == "author"),
        key=lambda n: metrics["pagerank"][n],
        reverse=True,
    )[:TOP_NODES]
    sources_sorted = sorted(
        (n for n, d in graph.nodes(data=True) if d["type"] == "source"),
        key=lambda n: metrics["pagerank"][n],
        reverse=True,
    )[:TOP_NODES]

    bridge_candidates = []
    for node in graph.nodes:
        weight_by_community = Counter()
        for member in graph.neighbors(node):
            cid = node_community.get(member)
            if cid:
                weight_by_community[cid] += graph[node][member]["weight"]
        total_weight = sum(weight_by_community.values())
        if len(weight_by_community) < 2 or total_weight == 0:
            continue
        ranked = weight_by_community.most_common()
        second_share = ranked[1][1] / total_weight
        if second_share < MIN_BRIDGE_SHARE:
            continue
        info = _node_info(graph, node, metrics, node_community, node_events)
        info["communities_connected"] = sorted(weight_by_community)
        info["bridge_share"] = round(second_share, 3)
        bridge_candidates.append(info)
    bridge_candidates.sort(key=lambda i: (-i["betweenness"], -i["bridge_share"]))
    bridge_ids = {item["node_id"] for item in bridge_candidates}

    for node in graph.nodes:
        graph.nodes[node]["community"] = node_community.get(node)
        graph.nodes[node]["pagerank"] = metrics["pagerank"][node]
        graph.nodes[node]["bridge"] = node in bridge_ids

    propagation = _propagation(topics or [], records, record_community, significant_ids) if topics else []

    multi_source_authors = sum(
        1 for n, d in graph.nodes(data=True) if d["type"] == "author" and graph.degree(n) >= 2
    )
    limitations = []
    if result["excluded_events"]:
        limitations.append(
            f"{result['excluded_events']} messages (News, or without a participant identity) are not part of the network."
        )
    if multi_source_authors == 0:
        limitations.append(
            "No participant appears in more than one source, so communities are single channels or videos and there are no bridge nodes yet. Collect more overlapping sources."
        )
    if any(item["platform"] == "youtube" for _, item in graph.nodes(data=True)):
        limitations.append("YouTube participants are identified by display name, which is not guaranteed to be unique.")

    result.update({
        "status": "ok",
        "authors": sum(1 for _, d in graph.nodes(data=True) if d["type"] == "author"),
        "sources": sum(1 for _, d in graph.nodes(data=True) if d["type"] == "source"),
        "edges": graph.number_of_edges(),
        "components": nx.number_connected_components(graph),
        "communities_total": len(partition),
        "communities_significant": len(significant_ids),
        "min_community_messages": MIN_COMMUNITY_MESSAGES,
        "communities": communities,
        "top_authors": [_node_info(graph, n, metrics, node_community, node_events) for n in authors_sorted],
        "top_sources": [_node_info(graph, n, metrics, node_community, node_events) for n in sources_sorted],
        "bridge_nodes": bridge_candidates[:TOP_NODES],
        "propagation": propagation,
        "limitations": limitations,
    })
    return result, graph