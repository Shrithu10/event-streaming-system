package com.eventstream.broker.group;

public enum GroupState {

    /** No members have joined yet. */
    EMPTY,

    /** A join or leave triggered reassignment; in-flight fetches are rejected. */
    REBALANCING,

    /** Assignment is stable; consumers may fetch. */
    STABLE
}
