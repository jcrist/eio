import enum
import random
from collections import namedtuple

Entry = namedtuple("Entry", ("index", "term", "item"))


class State(enum.IntEnum):
    FOLLOWER = 0
    CANDIDATE = 1
    LEADER = 2


class Msg(enum.IntEnum):
    APPEND_REQ = 0
    APPEND_RESP = 1
    VOTE_REQ = 2
    VOTE_RESP = 3

    @classmethod
    def append_req(cls, term, prev_log_index, prev_log_term, entries, leader_commit):
        return (
            cls.APPEND_REQ,
            term,
            prev_log_index,
            prev_log_term,
            entries,
            leader_commit
        )

    @classmethod
    def append_resp(cls, term, success):
        return (cls.APPEND_RESP, term, success)

    @classmethod
    def vote_req(cls, term, last_log_index, last_log_term):
        return (cls.VOTE_REQ, term, last_log_index, last_log_term)

    @classmethod
    def vote_resp(cls, term, success):
        return (cls.VOTE_RESP, term, success)


class Log(object):
    """A Raft log"""

    def __init__(self):
        self.entries = []
        self.offset = -1

    def append(self, entry):
        self.entries.append(entry)

    @property
    def last_index(self):
        if self.entries:
            return self.entries[-1].index
        return 0

    @property
    def last_term(self):
        if self.entries:
            return self.entries[-1].term
        return 0

    def lookup(self, index):
        i = index - self.offset
        if 0 <= i < len(self.entries):
            return self.entries[i]
        return None

    def rollback_to_before(self, index):
        assert index > 0
        i = index - self.offset
        if i >= 0:
            del self.entries[i:]


class Peer(object):
    """Information about a peer node"""
    def __init__(self, match_index=0, next_index=0):
        self.match_index = match_index
        self.next_index = next_index


class Node(object):
    def __init__(self, node_id, peer_node_ids, heartbeat_ticks=1, election_ticks=None,
                 random_state=None):
        self.node_id = node_id
        self.peers = {n: Peer() for n in peer_node_ids}

        # Random state
        if random_state is None:
            random_state = random.Random()
        self.random = random_state

        # Timing ticks
        if heartbeat_ticks <= 0:
            raise ValueError("heartbeat_ticks must be > 0")

        if election_ticks is None:
            election_ticks = 10 * heartbeat_ticks

        self.heartbeat_timeout = heartbeat_ticks
        self.election_ticks = election_ticks
        self.elapsed_ticks = 0
        self.reset_election_timeout()

        # Initialize as a follower
        self.state = State.FOLLOWER

        # Persistent state
        self.term = 0
        self.voted_for = None
        self.log = Log()

        # Volatile state
        self.commit_index = 0
        self.last_applied = 0

        self.handlers = {
            Msg.APPEND_REQ: self.on_append_req,
            Msg.APPEND_RESP: self.on_append_resp,
            Msg.VOTE_REQ: self.on_vote_req,
            Msg.VOTE_RESP: self.on_vote_resp,
        }

    def on_message(self, sender_id, msg):
        """Called when a new message is received.

        Returns
        -------
        msgs : list
            A list of ``(target, msg)`` pairs, where ``target`` is the node id
            to send to, and ``msg`` is the message to send.
        """
        return self.handlers[msg[0]](sender_id, *msg[1:])

    def on_tick(self):
        """Called on every time tick.

        Returns
        -------
        msgs : list
            A list of ``(target, msg)`` pairs, where ``target`` is the node id
            to send to, and ``msg`` is the message to send.
        """
        self.elapsed_ticks += 1

        msgs = []
        if self.state == State.LEADER:
            if self.elapsed_ticks >= self.heartbeat_timeout:
                self.reset_heartbeat_timeout()
                msgs = [
                    (node_id, self._make_append_req(peer))
                    for (node_id, peer) in self.peers.items()
                ]
        else:
            if self.elapsed_ticks >= self.election_timeout:
                self.reset_election_timeout()

                if self.state == State.FOLLOWER:
                    self.state = State.CANDIDATE

                self.term += 1
                self.voted_for = self.node_id
                self.vote_count = 1

                msgs = [
                    (
                        node,
                        Msg.vote_req(self.term, self.log.last_index, self.log.last_term)
                    )
                    for node in self.peers
                ]

        return msgs

    def reset_heartbeat_timeout(self):
        self.elapsed_ticks = 0

    def reset_election_timeout(self):
        # The election timeout is in [election_ticks, election_ticks * 2]
        self.elapsed_ticks = 0
        self.election_timeout = self.random.randint(
            self.election_ticks,
            2 * self.election_ticks
        )

    def reset(self):
        # Reset all timers
        self.reset_election_timeout()

        # Reset previous votes
        self.voted_for = None

        # Reset all peers
        for peer in self.peers.values():
            peer.vote = False
            peer.match_index = 0
            peer.next_index = self.log.last_index + 1

    def become_follower(self):
        self.reset()
        self.state = State.FOLLOWER

    def become_candidate(self):
        self.reset()
        self.term += 1
        self.voted_for = self.node_id
        self.state = State.CANDIDATE

    def become_leader(self):
        self.reset()
        self.state = State.LEADER

    def update_commit_index(self):
        for N in range(self.commit_index + 1, self.log.last_index + 1):
            count = len(p for p in self.peers.values() if p.match_index >= N)
            entry = self.log.lookup(N)
            entry_term = entry.term if entry else 0
            if self.is_majority(count) and self.term == entry_term:
                self.commit_index = N

    def update_last_applied(self):
        for index in range(self.last_applied + 1, self.commit_index + 1):
            entry = self.lookup(index)
            self.apply_entry(entry)
            self.last_applied += 1

    def apply_entry(self, item):
        pass

    def maybe_become_follower(self, term):
        if term > self.term:
            self.term = term
            self.become_follower()

    def is_majority(self, n):
        return n >= len(self.peers) / 2

    def _make_append_req(self, peer):
        prev_index = peer.next_index - 1
        prev_entry = self.log.lookup(prev_index)
        prev_term = prev_entry.term if prev_entry is not None else 0

        if self.log.last_index >= peer.next_index:
            entries = [self.log.lookup(peer.next_index)]
        else:
            entries = []

        return Msg.append_req(
            self.term, prev_index, prev_term, entries, self.commit_index
        )

    def on_append_req(
        self, node_id, term, prev_log_index, prev_log_term, entries, leader_commit
    ):
        if term < self.term:
            reply = Msg.append_resp(self.term, False)
            return [(node_id, reply)]

        self.maybe_become_follower(term)

        if prev_log_index > 0:
            existing = self.log.lookup(prev_log_index)
            if existing is None or existing.term != prev_log_term:
                reply = Msg.append_resp(self.term, False)
                return [(node_id, reply)]

        if entries:
            for e_index, e_term, e_item in entries:
                existing = self.log.lookup(e_index)
                if existing is None:
                    self.log.append(Entry(e_index, e_term, e_item))
                elif existing.term != e_term:
                    self.log.rollback_to_before(e_index)
                    self.log.append(Entry(e_index, e_term, e_item))

            if leader_commit > self.commit_index:
                self.commit_index = min(leader_commit, self.log.last_index)

            self.update_last_applied()

        self.reset_election_timeout()

        reply = Msg.append_resp(self.term, True)
        return [(node_id, reply)]

    def on_append_resp(self, node_id, term, success):
        self.maybe_become_follower(term)

        msgs = []
        if self.state == State.LEADER:
            peer = self.peers[node_id]
            if success:
                peer.next_index = self.log.last_index + 1
                peer.match_index = self.log.last_index
                self.update_commit_index()
                self.update_last_applied()
            elif peer.next_index > 1:
                peer.next_index -= 1

            if self.log.last_index >= peer.next_index:
                req = self._make_append_req(peer)
                msgs = [(node_id, req)]
        return msgs

    def on_vote_req(
        self, node_id, term, last_log_index, last_log_term
    ):
        if term < self.term:
            reply = Msg.vote_resp(self.term, False)
            return [(node_id, reply)]

        self.maybe_become_follower(term)

        if (self.voted_for is None or self.voted_for == node_id) and (
            last_log_index >= self.log.last_index
            and last_log_term >= self.log.last_term
        ):
            self.reset_election_timeout()
            self.voted_for = node_id
            return [(node_id, Msg.vote_resp(self.term, True))]
        else:
            return [(node_id, Msg.vote_resp(self.term, False))]

    def on_vote_resp(self, node_id, term, success):
        self.maybe_become_follower(term)

        msgs = []
        if self.state == State.CANDIDATE:
            if success:
                self.vote_count += 1
                if self.vote_count >= self.is_majority(self.vote_count):
                    self.state = State.LEADER
                    for peer in self.peers.values():
                        peer.next_index = self.log.last_index + 1
                        peer.match_index = 0
                msgs = [
                    (node_id, self._make_append_req(peer))
                    for (node_id, peer) in self.peers.items()
                ]
        return msgs