import asyncio
import enum
import random
import logging

from collections import namedtuple, deque, defaultdict

from .utils import id_generator, prefix_for_id


def get_default_logger():
    log = logging.getLogger(__name__)
    log.setLevel(logging.INFO)
    log.propagate = False
    if log.hasHandlers():
        return
    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        fmt="%(asctime)s.%(msecs)03d %(levelname)-8s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler.setFormatter(formatter)
    log.addHandler(handler)
    return log


ReadOnly = namedtuple(
    "ReadOnly", ("node_id", "req_id", "index", "resps", "item", "future")
)

Entry = namedtuple("Entry", ("index", "term", "item"))

Snapshot = namedtuple("Snapshot", ("data", "index", "term"))


class State(enum.IntEnum):
    FOLLOWER = 0
    CANDIDATE = 1
    LEADER = 2


class PeerState(enum.IntEnum):
    PROBE = 0
    REPLICATE = 1
    SNAPSHOT = 2


class Msg(enum.IntEnum):
    APPEND_REQ = 0
    APPEND_RESP = 1
    VOTE_REQ = 2
    VOTE_RESP = 3
    PROPOSE_REQ = 4
    PROPOSE_RESP = 5
    READINDEX_REQ = 6
    READINDEX_RESP = 7
    SNAPSHOT_REQ = 8
    SNAPSHOT_RESP = 9


class Log(object):
    """A Raft log"""

    def __init__(self):
        self.entries = []
        self.offset = 0

    def append(self, index, term, item):
        self.entries.append(Entry(index, term, item))

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
        i = index - self.offset - 1
        if 0 <= i < len(self.entries):
            return self.entries[i]
        return None

    def reset(self, offset=0):
        self.entries = []
        self.offset = offset

    def rollback_to_before(self, index):
        assert index > 0
        i = index - self.offset - 1
        if i >= 0:
            del self.entries[i:]

    def drop_entries(self, index):
        i = index - self.offset
        assert i > 0
        self.entries = self.entries[i:]
        self.offset = index


class StateMachine(object):
    def apply(self, item):
        pass

    def apply_read(self, item):
        pass

    def create_snapshot(self):
        pass

    def restore_snapshot(self, snap):
        pass


class Peer(object):
    """Information about a peer node"""

    def __init__(self, node_id):
        self.node_id = node_id
        self.state = PeerState.PROBE
        self.match_index = 0
        self.next_index = 0
        self.voted = False
        self.done_requests = set()
        self.recently_messaged = False

    def __repr__(self):
        return "Peer<match_index=%d, next_index=%d>" % (
            self.match_index,
            self.next_index,
        )

    def reset(self, last_index):
        self.state = PeerState.PROBE
        self.match_index = 0
        self.next_index = last_index + 1
        self.voted = False

    def become_probe(self):
        self.state = PeerState.PROBE
        self.next_index = self.match_index + 1

    def become_replicate(self):
        self.state = PeerState.REPLICATE
        self.next_index = self.match_index + 1

    def become_snapshot(self):
        self.state = PeerState.SNAPSHOT

    def maybe_update_index(self, index):
        update = self.match_index < index
        if update:
            self.match_index = index
        if self.next_index < index + 1:
            self.next_index = index + 1
        return update

    def optimistic_update_index(self, n):
        self.next_index = n + 1

    def maybe_decrement_index(self, rejected):
        if self.state == PeerState.REPLICATE:
            if self.match_index >= rejected:
                return False
            else:
                self.next_index = self.match_index + 1
                return True
        else:
            self.next_index = max(rejected - 1, 1)
            return True

    def request_already_done(self, req_id):
        return req_id in self.done_requests

    def add_done_request(self, req_id):
        self.done_requests.add(req_id)

    def update_done_requests(self, last_req_id):
        # TODO: more efficient data structure
        self.done_requests = set(i for i in self.done_requests if i > last_req_id)


class RaftNode(object):
    def __init__(
        self,
        node_id,
        peer_node_ids,
        state_machine,
        heartbeat_ticks=1,
        election_ticks=None,
        max_entries_per_msg=5,
        snapshot_entries=1000,
        snapshot_overlap_entries=10,
        random_state=None,
        logger=None,
        loop=None,
    ):
        self.node_id = node_id
        self.peers = {n: Peer(n) for n in peer_node_ids}
        self.pending = {}
        self.last_req_id = 0
        self.leader_id = None
        self.req_id_generator = id_generator(self.node_id)
        self.state_machine = state_machine
        self.loop = loop or asyncio.get_event_loop()
        self.max_entries_per_msg = max_entries_per_msg
        self.snapshot_entries = snapshot_entries
        self.snapshot_overlap_entries = snapshot_overlap_entries
        assert snapshot_overlap_entries < snapshot_entries

        # Read-only requests
        # - A queue of ReadOnly objects
        self.readonly_queue = deque()
        # - A map of request_id -> ReadOnly objects
        self.readonly_map = {}
        # - A map of readindex -> List[ReadOnly]
        self.readindex_map = defaultdict(list)

        # Random state
        if random_state is None:
            random_state = random.Random()
        self.random = random_state

        # Logging
        self.logger = logger or get_default_logger()

        # Timing ticks
        if heartbeat_ticks <= 0:
            raise ValueError("heartbeat_ticks must be > 0")

        if election_ticks is None:
            election_ticks = 10 * heartbeat_ticks

        self.heartbeat_timeout = heartbeat_ticks
        self.election_ticks = election_ticks
        self.elapsed_ticks = 0
        self.reset_election_timeout()

        self.term = 0
        self.log = Log()
        self.snapshot = None

        self.commit_index = 0
        self.applied_index = 0

        self.handlers = {
            Msg.APPEND_REQ: self.on_append_req,
            Msg.APPEND_RESP: self.on_append_resp,
            Msg.VOTE_REQ: self.on_vote_req,
            Msg.VOTE_RESP: self.on_vote_resp,
            Msg.PROPOSE_REQ: self.on_propose_req,
            Msg.PROPOSE_RESP: self.on_propose_resp,
            Msg.READINDEX_REQ: self.on_readindex_req,
            Msg.READINDEX_RESP: self.on_readindex_resp,
            Msg.SNAPSHOT_REQ: self.on_snapshot_req,
            Msg.SNAPSHOT_RESP: self.on_snapshot_resp,
        }

        # Initialize as a follower
        self.become_follower()

    def append_req(
        self, term, prev_log_index, prev_log_term, entries, leader_commit, tag
    ):
        return (
            Msg.APPEND_REQ,
            self.node_id,
            term,
            prev_log_index,
            prev_log_term,
            entries,
            leader_commit,
            tag,
        )

    def append_resp(self, term, success, index, tag):
        return (Msg.APPEND_RESP, self.node_id, term, success, index, tag)

    def vote_req(self, term, last_log_index, last_log_term):
        return (Msg.VOTE_REQ, self.node_id, term, last_log_index, last_log_term)

    def vote_resp(self, term, success):
        return (Msg.VOTE_RESP, self.node_id, term, success)

    def propose_req(self, req_id, item, last_req_id):
        return (Msg.PROPOSE_REQ, self.node_id, req_id, item, last_req_id)

    def propose_resp(self, req_id, item, leader_id):
        return (Msg.PROPOSE_RESP, self.node_id, req_id, item, leader_id)

    def readindex_req(self, req_id):
        return (Msg.READINDEX_REQ, self.node_id, req_id)

    def readindex_resp(self, req_id, index, leader_id):
        return (Msg.READINDEX_RESP, self.node_id, req_id, index, leader_id)

    def snapshot_req(self, term, snap_index, snap_term, snap_data):
        return (Msg.SNAPSHOT_REQ, self.node_id, term, snap_index, snap_term, snap_data)

    def snapshot_resp(self, term, success, index):
        return (Msg.SNAPSHOT_RESP, self.node_id, term, success, index)

    def create_future(self):
        return self.loop.create_future()

    def next_request_id(self):
        return next(self.req_id_generator)

    def on_message(self, msg):
        """Called when a new message is received.

        Returns
        -------
        msgs : list
            A list of ``(target, msg)`` pairs, where ``target`` is the node id
            to send to, and ``msg`` is the message to send.
        """
        return self.handlers[msg[0]](*msg[1:])

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
                msgs = self.broadcast_append(is_heartbeat=True)
        else:
            if self.elapsed_ticks >= self.election_timeout:
                self.become_candidate()
                msgs = [
                    (
                        node,
                        self.vote_req(
                            self.term, self.log.last_index, self.log.last_term
                        ),
                    )
                    for node in self.peers
                ]

        return msgs

    def propose(self, item):
        req_id = self.next_request_id()
        future = self.create_future()
        self.pending[req_id] = future

        self.logger.debug("Creating proposal [req_id: %d, item: %r]", req_id, item)

        if self.leader_id is None:
            msgs = self.on_propose_resp(self.node_id, req_id, item, None)
        elif self.state == State.LEADER:
            msgs = self.on_propose_req(self.node_id, req_id, item, self.last_req_id)
        else:
            msgs = [(self.leader_id, self.propose_req(req_id, item, self.last_req_id))]

        return future, msgs

    def read(self, item, local=False):
        if local:
            future = self.create_future()
            res = self.state_machine.apply_read(item)
            future.set_result(res)
            msgs = []
        else:
            req_id = self.next_request_id()
            future = self.create_future()

            self.logger.debug(
                "Creating read-only request [req_id: %d, item: %r]", req_id, item
            )

            if self.leader_id is None:
                future.set_exception(ValueError("Leader unknown"))
                msgs = []
            elif self.state == State.LEADER:
                if self.has_committed_entry_this_term():
                    msgs = self.schedule_readonly(
                        self.node_id, req_id, item=item, future=future
                    )
                else:
                    future.set_exception(
                        ValueError("Leader hasn't committed anything yet this term")
                    )
                    msgs = []
            else:
                ro = ReadOnly(
                    node_id=self.node_id,
                    req_id=req_id,
                    index=None,
                    resps=None,
                    item=item,
                    future=future,
                )
                self.readonly_map[ro.req_id] = ro
                msgs = [(self.leader_id, self.readindex_req(req_id))]

        return future, msgs

    def reset_heartbeat_timeout(self):
        self.elapsed_ticks = 0

    def reset_election_timeout(self):
        # The election timeout is in [election_ticks, election_ticks * 2]
        self.elapsed_ticks = 0
        self.election_timeout = self.random.randint(
            self.election_ticks, 2 * self.election_ticks
        )

    def reset_client_requests(self):
        exc = ValueError("Leadership changed during request, please retry")
        # Reset any pending read requests
        self.readonly_queue.clear()
        for m in [self.readonly_map, self.readindex_map]:
            for ro in m.values():
                if ro.future is not None and not ro.future.done():
                    ro.set_exception(exc)
            m.clear()

        # Reset any pending propose requests
        for f in self.pending.values():
            f.set_exception(exc)
        self.pending.clear()

    def reset(self):
        # Reset all timers
        self.reset_election_timeout()

        # No leader known
        self.leader_id = None

        # Reset previous votes
        self.voted_for = None
        self.vote_count = 0

        # Reset all peers
        for peer in self.peers.values():
            peer.reset(self.log.last_index)

        # Reset client requests
        self.reset_client_requests()

    def become_follower(self, leader_id=None):
        self.reset()
        self.state = State.FOLLOWER
        self.leader_id = leader_id

        self.logger.info(
            "Server %s transitioned to follower, term %s", self.node_id, self.term
        )

    def become_candidate(self):
        self.reset()
        self.state = State.CANDIDATE
        self.leader_id = None
        self.term += 1
        self.voted_for = self.node_id
        self.vote_count = 1

        self.logger.info(
            "Server %s transitioned to candidate, term %s", self.node_id, self.term
        )

    def become_leader(self):
        self.reset()
        self.state = State.LEADER
        self.leader_id = self.node_id

        self.logger.info(
            "Server %s transitioned to leader, term %s", self.node_id, self.term
        )

    def update_commit_index(self):
        updated = False
        for N in range(self.commit_index + 1, self.log.last_index + 1):
            count = sum(1 for p in self.peers.values() if p.match_index >= N)
            entry = self.log.lookup(N)
            entry_term = entry.term if entry else 0
            if self.is_majority(count) and self.term == entry_term:
                self.commit_index = N
                updated = True
        return updated

    def update_last_applied(self):
        for index in range(self.applied_index + 1, self.commit_index + 1):
            entry = self.log.lookup(index)
            self.apply_entry(entry)
            self.applied_index += 1
            # Apply any pending reads
            for ro in self.readindex_map.pop(self.applied_index, ()):
                res = self.state_machine.apply_read(ro.item)
                ro.future.set_result(res)
            # Maybe take a snapshot
            if self.applied_index - self.log.offset > self.snapshot_entries:
                self.logger.info("Creating snapshot up to index %d", self.applied_index)
                self.snapshot = Snapshot(
                    data=self.state_machine.create_snapshot(),
                    index=self.log.last_index,
                    term=self.log.last_term,
                )
                self.log.drop_entries(
                    self.applied_index - self.snapshot_overlap_entries
                )

    def apply_entry(self, entry):
        if entry.item is None:
            return
        req_id, item, last_req_id = entry.item
        self.logger.debug(
            "Applying entry [req_id: %d, item: %r, last_req_id: %d]",
            req_id,
            item,
            last_req_id,
        )
        node_id = prefix_for_id(req_id)
        if node_id == self.node_id:
            if req_id > self.last_req_id:
                resp = self.state_machine.apply(item)
                fut = self.pending.pop(req_id, None)
                if fut and not fut.done():
                    fut.set_result(resp)
                self.last_req_id = req_id
            else:
                assert req_id not in self.pending
        else:
            peer = self.peers.get(node_id)
            if peer is not None and not peer.request_already_done(req_id):
                self.state_machine.apply(item)
                peer.add_done_request(req_id)

    def schedule_readonly(self, node_id, req_id, item=None, future=None):
        ro = ReadOnly(
            node_id=node_id,
            req_id=req_id,
            index=self.commit_index,
            resps=set(),
            item=item,
            future=future,
        )
        self.readonly_map[ro.req_id] = ro
        self.readonly_queue.append(ro)
        return self.broadcast_append(send_empty=True)

    def process_readonly(self, ro, index=None):
        if ro.future.done():
            return

        if index is None:
            index = ro.index

        if index <= self.applied_index:
            # Read is ready now, apply
            res = self.state_machine.apply_read(ro.item)
            ro.future.set_result(res)
        else:
            # Read will be ready later
            self.readindex_map[index].append(ro)

    def maybe_become_follower(self, term, leader_id=None):
        if term > self.term:
            self.term = term
            self.become_follower(leader_id)

    def is_majority(self, n):
        return n >= len(self.peers) / 2

    def has_committed_entry_this_term(self):
        committed = self.log.lookup(self.commit_index)
        return committed is not None and committed.term == self.term

    def _make_append_reqs(self, peer, is_heartbeat=False, send_empty=False):
        if is_heartbeat and peer.recently_messaged:
            peer.recently_messaged = False
            return []

        if peer.state == PeerState.SNAPSHOT:
            # When in snapshot state, we hold off on messages until we receive
            # a reply
            return []

        prev_index = peer.next_index - 1
        should_snapshot = False

        if self.log.offset < peer.next_index:
            if self.log.offset < prev_index:
                prev_term = self.log.lookup(prev_index).term
            else:
                assert self.log.offset == prev_index
                if self.log.offset == 0:
                    prev_term = 0
                elif self.snapshot is not None:
                    prev_term = self.snapshot.term
                else:
                    should_snapshot = True
        else:
            should_snapshot = True

        if should_snapshot:
            peer.become_snapshot()
            req = self.snapshot_req(
                self.term, self.snapshot.index, self.snapshot.term, self.snapshot.data
            )
            return [(peer.node_id, req)]

        if peer.state == PeerState.PROBE:
            if self.log.last_index >= peer.next_index:
                entries = [self.log.lookup(peer.next_index)]
            else:
                entries = []
        else:
            entries = [
                self.log.lookup(i)
                for i in range(
                    peer.next_index,
                    min(self.max_entries_per_msg + peer.next_index, self.log.last_index)
                    + 1,
                )
            ]
            if entries:
                peer.optimistic_update_index(entries[-1].index)

        if not entries and not is_heartbeat and not send_empty:
            return []

        peer.recently_messaged = not is_heartbeat

        if self.readonly_queue:
            tag = self.readonly_queue[-1].req_id
        else:
            tag = None

        req = self.append_req(
            self.term, prev_index, prev_term, entries, self.commit_index, tag
        )
        return [(peer.node_id, req)]

    def broadcast_append(self, is_heartbeat=False, send_empty=False):
        msgs = []
        for peer in self.peers.values():
            msgs.extend(
                self._make_append_reqs(
                    peer, is_heartbeat=is_heartbeat, send_empty=send_empty
                )
            )
        return msgs

    def on_append_req(
        self, node_id, term, prev_log_index, prev_log_term, entries, leader_commit, tag
    ):
        self.logger.debug(
            "Received append request: [node_id: %d, term: %d, prev_log_index: %d, "
            "prev_log_term: %d, entries: %r, leader_commit: %d]",
            node_id,
            term,
            prev_log_index,
            prev_log_term,
            entries,
            leader_commit,
        )
        # Reject requests with a previous term
        if term < self.term:
            reply = self.append_resp(self.term, False, None, tag)
            return [(node_id, reply)]

        # Requests with a higher term may convert this node to a follower
        self.maybe_become_follower(term, node_id)

        if self.state == State.FOLLOWER:
            self.leader_id = node_id
            self.reset_election_timeout()

            if prev_log_index > self.log.offset:
                existing = self.log.lookup(prev_log_index)
                if existing is None or existing.term != prev_log_term:
                    reply = self.append_resp(self.term, False, prev_log_index + 1, tag)
                    return [(node_id, reply)]

            for e_index, e_term, e_item in entries:
                existing = self.log.lookup(e_index)
                if existing is None:
                    self.log.append(e_index, e_term, e_item)
                elif existing.term != e_term:
                    self.log.rollback_to_before(e_index)
                    self.log.append(e_index, e_term, e_item)

            if leader_commit > self.commit_index:
                self.commit_index = min(leader_commit, self.log.last_index)
                self.update_last_applied()

            reply = self.append_resp(self.term, True, self.log.last_index, tag)
            return [(node_id, reply)]
        else:
            return []

    def on_append_resp(self, node_id, term, success, index, tag):
        self.logger.debug(
            "Received append response: [node_id: %d, term: %d, success: %s, index: %s]",
            node_id,
            term,
            success,
            index,
        )
        self.maybe_become_follower(term, node_id)

        if self.state != State.LEADER:
            return []

        msgs = []

        peer = self.peers[node_id]
        ro = self.readonly_map.get(tag)
        if ro is not None:
            ro.resps.add(node_id)
            if self.is_majority(len(ro.resps) + 1):
                # We have now heard from a majority of nodes that the index is
                # this far, which means all read requests up to `tag` have been
                # accounted for.
                while self.readonly_queue:
                    ro = self.readonly_queue.popleft()
                    del self.readonly_map[ro.req_id]

                    if ro.future is not None:
                        self.process_readonly(ro)
                    else:
                        # Remote request, return read index
                        reply = self.readindex_resp(ro.req_id, ro.index, None)
                        msgs.append((ro.node_id, reply))

                    if ro.req_id == tag:
                        break

        if success:
            if peer.maybe_update_index(index):
                if peer.state == PeerState.PROBE:
                    peer.become_replicate()

                if self.update_commit_index():
                    self.update_last_applied()
                    msgs.extend(self.broadcast_append())
        else:
            if peer.maybe_decrement_index(index):
                if peer.state == PeerState.REPLICATE:
                    peer.become_probe()

        msgs.extend(self._make_append_reqs(peer))

        return msgs

    def on_vote_req(self, node_id, term, last_log_index, last_log_term):
        self.logger.debug(
            "Received vote request: [node_id: %d, term: %d, "
            "last_log_index: %d, last_log_term: %d]",
            node_id,
            term,
            last_log_index,
            last_log_term,
        )
        if term < self.term:
            reply = self.vote_resp(self.term, False)
            return [(node_id, reply)]

        self.maybe_become_follower(term, node_id)

        if (self.voted_for is None or self.voted_for == node_id) and (
            last_log_index >= self.log.last_index
            and last_log_term >= self.log.last_term
        ):
            self.reset_election_timeout()
            self.voted_for = node_id
            return [(node_id, self.vote_resp(self.term, True))]
        else:
            return [(node_id, self.vote_resp(self.term, False))]

    def on_vote_resp(self, node_id, term, success):
        self.logger.debug(
            "Received vote response: [node_id: %d, term: %d, success: %s]",
            node_id,
            term,
            success,
        )
        self.maybe_become_follower(term, node_id)

        msgs = []
        if self.state == State.CANDIDATE:
            peer = self.peers[node_id]
            if not peer.voted:
                peer.voted = True
                if success:
                    self.vote_count += 1
                    if self.vote_count >= self.is_majority(self.vote_count):
                        self.become_leader()
                        # We append an empty entry on leadership transition
                        index = self.log.last_index + 1
                        self.log.append(index, self.term, None)
                        msgs = self.broadcast_append()
        return msgs

    def on_propose_req(self, node_id, req_id, item, last_req_id):
        self.logger.debug(
            "Received propose request: [node_id: %d, req_id: %d, "
            "item: %r, last_req_id: %d]",
            node_id,
            req_id,
            item,
            last_req_id,
        )
        if self.state == State.LEADER:
            # This node thinks it is the leader, apply directly
            index = self.log.last_index + 1
            self.log.append(index, self.term, (req_id, item, last_req_id))
            msgs = self.broadcast_append()
        else:
            # We're not the leader, respond accordingly
            msgs = [(node_id, self.propose_resp(req_id, item, self.leader_id))]

        return msgs

    def on_propose_resp(self, node_id, req_id, item, leader_id):
        self.logger.debug(
            "Received propose response: [node_id: %d, req_id: %d, "
            "item: %r, leader_id: %d]",
            node_id,
            req_id,
            item,
            leader_id,
        )
        fut = self.pending.get(req_id)
        msgs = []
        if fut is not None and not fut.done():
            if leader_id is not None:
                # Retry with new leader
                msgs = [(leader_id, self.propose_req(req_id, item, self.last_req_id))]
            else:
                fut.set_exception(ValueError("Leader unknown"))
                del self.pending[req_id]
                if req_id > self.last_req_id:
                    self.last_req_id = req_id
        else:
            # Future already done, move on
            if fut is not None:
                del self.pending[req_id]
            if req_id > self.last_req_id:
                self.last_req_id = req_id
        return msgs

    def on_readindex_req(self, node_id, req_id):
        self.logger.debug(
            "Received readindex request: [node_id: %d, req_id: %d]", node_id, req_id
        )
        if self.state == State.LEADER and self.has_committed_entry_this_term():
            msgs = self.schedule_readonly(node_id, req_id)
        else:
            # We're either not the leader, or we are the leader but aren't
            # quite ready to take requests. Respond accordingly.
            msgs = [(node_id, self.readindex_resp(req_id, None, self.leader_id))]

        return msgs

    def on_readindex_resp(self, node_id, req_id, index, leader_id):
        self.logger.debug(
            "Received readindex response: [node_id: %d, req_id: %d, "
            "index: %s, leader_id: %s]",
            node_id,
            req_id,
            index,
        )
        ro = self.readonly_map.pop(req_id, None)
        if ro is not None and not ro.future.done():
            if index is None:
                if leader_id is None:
                    # Leader unknown, error
                    ro.future.set_exception(ValueError("Leader unknown"))
                else:
                    # Retry with new leader
                    self.readonly_map[ro.req_id] = ro
                    return [(leader_id, self.readindex_req(req_id))]
            else:
                self.process_readonly(ro, index=index)

        return []

    def on_snapshot_req(self, node_id, term, snap_index, snap_term, snap_data):
        self.logger.debug(
            "Received snapshot request: [node_id: %d, term: %d, "
            "snap_index: %d, snap_term: %d]",
            node_id,
            term,
            snap_index,
            snap_term,
        )
        # Reject requests with a previous term
        if term < self.term:
            reply = self.snapshot_resp(self.term, False, self.commit_index)
            return [(node_id, reply)]

        self.maybe_become_follower(term, node_id)

        if self.state == State.FOLLOWER:
            self.reset_election_timeout()
            if self.commit_index >= snap_index:
                success = False
            else:
                self.leader_id = node_id
                self.state_machine.restore_snapshot(snap_data)
                self.log.reset(snap_index)
                self.commit_index = snap_index
                self.applied_index = snap_index
                success = True
        else:
            success = False

        reply = self.snapshot_resp(self.term, success, self.commit_index)
        return [(node_id, reply)]

    def on_snapshot_resp(self, node_id, term, success, index):
        self.logger.debug(
            "Received snapshot response: [node_id: %d, term: %d, "
            "success: %s, index: %d]",
            node_id,
            term,
            success,
            index,
        )
        peer = self.peers[node_id]
        if success:
            peer.match_index = index
            peer.become_probe()

        return self._make_append_reqs(peer)
