import networkx as nx

class BlockchainGraphEngine:
    def __init__(self, mixer_address: str):
        # Initialize an empty directed graph (DiGraph) because money flow has direction
        self.G = nx.DiGraph()
        self.mixer_address = mixer_address

    def add_transaction(self, sender: str, receiver: str, amount: float, timestamp: int):
        """
        Dynamically inserts wallets as nodes and transactions as edges.
        Accumulates volume and records metadata in real-time.
        """
        # Ensure nodes exist
        if not self.G.has_node(sender):
            self.G.add_node(sender, first_seen=timestamp)
        if not self.G.has_node(receiver):
            self.G.add_node(receiver, first_seen=timestamp)

        # If an edge (transaction path) already exists, update its properties
        if self.G.has_edge(sender, receiver):
            self.G[sender][receiver]['weight'] += amount
            self.G[sender][receiver]['tx_count'] += 1
        else:
            # Create a new directed edge
            self.G.add_edge(sender, receiver, weight=amount, tx_count=1, timestamp=timestamp)

    def calculate_distance_from_mixer(self, target_wallet: str, max_hops: int = 5) -> int:
        """
        Uses Breadth-First Search (BFS) to find the shortest degree of separation
        between a known privacy mixer (Tornado Cash) and the target wallet.
        Returns the number of hops, or -1 if no connection exists within max_hops.
        """
        # If the wallet itself is the mixer, distance is zero
        if target_wallet == self.mixer_address:
            return 0
            
        # If the target or mixer isn't in our graph yet, no path is possible
        if not self.G.has_node(self.mixer_address) or not self.G.has_node(target_wallet):
            return -1

        try:
            # Compute the shortest path length in the directed graph
            # Since money flows MIXER -> WALLET A -> WALLET B, we check path from mixer to target
            path_length = nx.shortest_path_length(self.G, source=self.mixer_address, target=target_wallet)
            
            if path_length <= max_hops:
                return path_length
            return -1
        except nx.NetworkXNoPath:
            return -1

    def get_node_centrality(self, wallet: str) -> float:
        """
        Calculates out-degree centrality to evaluate if a wallet behaves like 
        a high-volume laundering hub (distributing funds to many targets rapidly).
        """
        if not self.G.has_node(wallet):
            return 0.0
        
        # Out-degree centrality = fraction of nodes its outgoing edges connected to
        centrality_dict = nx.out_degree_centrality(self.G)
        return centrality_dict.get(wallet, 0.0)

    def get_graph_stats(self):
        """Returns diagnostic metrics to display on the dashboard"""
        return {
            "total_wallets": self.G.number_of_nodes(),
            "total_edges": self.G.number_of_edges()
        }