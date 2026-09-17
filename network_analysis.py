from pymongo import MongoClient
import networkx as nx
from pyvis.network import Network
import os
from dotenv import load_dotenv

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")

mongo_client = MongoClient(MONGO_URI)
db = mongo_client["pulse_db"]


def build_bipartite_graph():
    G = nx.Graph()

    for doc in db["telegram_sentiment_results"].find():
        channel = doc.get("channel_username") or doc.get("channel_title")
        if not channel:
            continue
        channel_node = f"channel::{channel}"
        G.add_node(channel_node, type="channel", label=doc.get("channel_title", channel))

        for m in doc.get("messages", []):
            sender = m.get("sender_id")
            if sender is None:
                continue
            sender_node = f"user::{sender}"
            G.add_node(sender_node, type="user", label=f"User {sender}")
            if G.has_edge(sender_node, channel_node):
                G[sender_node][channel_node]["weight"] += 1
            else:
                G.add_edge(sender_node, channel_node, weight=1)

    return G


def compute_influence(G):
    user_nodes = [n for n, d in G.nodes(data=True) if d.get("type") == "user"]
    channel_nodes = [n for n, d in G.nodes(data=True) if d.get("type") == "channel"]

    influence = {n: G.degree(n) for n in user_nodes}
    reach = {n: G.degree(n) for n in channel_nodes}

    top_users = sorted(influence.items(), key=lambda x: x[1], reverse=True)[:10]
    top_channels = sorted(reach.items(), key=lambda x: x[1], reverse=True)[:10]

    return top_users, top_channels


def visualize(G, output_file="network_graph.html"):
    net = Network(height="750px", width="100%", bgcolor="#FAF9F6", font_color="#2C2C2A")
    for node, data in G.nodes(data=True):
        color = "#639922" if data.get("type") == "channel" else "#3B8AD9"
        size = 25 if data.get("type") == "channel" else 12
        net.add_node(node, label=data.get("label", node), color=color, size=size)
    for source, target, data in G.edges(data=True):
        net.add_edge(source, target, value=data.get("weight", 1))

    net.show(output_file, notebook=False)


if __name__ == "__main__":
    print("Building network graph from Telegram data...\n")
    G = build_bipartite_graph()
    print(f"Graph built: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges\n")

    if G.number_of_nodes() == 0:
        print("No data found. Run telegram_fetch.py and telegram_sentiment.py first.")
    else:
        top_users, top_channels = compute_influence(G)

        print("=" * 60)
        print("TOP INFLUENTIAL USERS (active across multiple channels)")
        print("=" * 60)
        for user, score in top_users:
            print(f"{user:25s} present in {score} channel(s)")

        print("\n" + "=" * 60)
        print("TOP CHANNELS BY REACH (most unique participants)")
        print("=" * 60)
        for channel, score in top_channels:
            print(f"{channel:25s} {score} unique user(s)")

        visualize(G)
        print("\nInteractive network graph saved to network_graph.html — open it in your browser.")