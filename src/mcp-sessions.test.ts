import assert from "node:assert/strict";
import { McpSessionRegistry } from "./mcp-sessions.js";

interface FakeTransport {
  closeCalls: number;
  close(): Promise<void>;
}

function createTransport(closeError?: Error): FakeTransport {
  return {
    closeCalls: 0,
    async close() {
      this.closeCalls += 1;
      if (closeError) throw closeError;
    },
  };
}

let now = 0;
const registry = new McpSessionRegistry<FakeTransport>({ now: () => now });
const staleTransport = createTransport();
const activeTransport = createTransport();

registry.register("stale", staleTransport);
now = 1_000;
registry.register("active", activeTransport);
now = 1_500;
assert.equal(registry.get("active"), activeTransport);
now = 2_000;

const idleResults = await registry.closeIdle(1_500);
assert.deepEqual(idleResults, [{ sessionId: "stale" }]);
assert.equal(staleTransport.closeCalls, 1);
assert.equal(activeTransport.closeCalls, 0);
assert.equal(registry.size, 1);
assert.equal(registry.get("stale"), undefined);
assert.equal(registry.get("active"), activeTransport);

const closeError = new Error("close failed");
const failingTransport = createTransport(closeError);
registry.register("failing", failingTransport);
now = 10_000;

const failingResults = await registry.closeIdle(1);
assert.equal(failingResults.length, 2);
assert.deepEqual(failingResults.map((result) => result.sessionId).sort(), ["active", "failing"]);
assert.equal(failingResults.find((result) => result.sessionId === "failing")?.error, closeError);
assert.equal(failingTransport.closeCalls, 1);
assert.equal(registry.size, 0);

const first = createTransport();
const second = createTransport();
registry.register("first", first);
registry.register("second", second);
registry.remove("first");

const shutdownResults = await registry.closeAll();
assert.deepEqual(shutdownResults, [{ sessionId: "second" }]);
assert.equal(first.closeCalls, 0);
assert.equal(second.closeCalls, 1);
assert.equal(registry.size, 0);

let finishDelayedClose: (() => void) | undefined;
let delayedCloseResolved = false;
const delayedTransport: FakeTransport = {
  closeCalls: 0,
  close() {
    this.closeCalls += 1;
    return new Promise<void>((resolve) => {
      finishDelayedClose = resolve;
    });
  },
};
registry.register("delayed", delayedTransport);
const delayedClose = registry.closeAll();
void delayedClose.then(() => {
  delayedCloseResolved = true;
});

await Promise.resolve();
assert.equal(delayedCloseResolved, false);
assert.equal(delayedTransport.closeCalls, 1);
finishDelayedClose?.();
await delayedClose;
assert.equal(delayedCloseResolved, true);
assert.equal(registry.size, 0);

now = 0;
const bounded = new McpSessionRegistry<FakeTransport>({
  now: () => now,
  maxSessions: 2,
});
const boundedFirst = createTransport();
const boundedSecond = createTransport();
const boundedThird = createTransport();

const firstReservation = await bounded.reserve();
assert.ok(firstReservation);
assert.equal(bounded.occupiedCapacity, 1);
bounded.commit(firstReservation, "first", boundedFirst);
bounded.markIdle("first");
now = 1;

const secondReservation = await bounded.reserve();
assert.ok(secondReservation);
bounded.commit(secondReservation, "second", boundedSecond);
bounded.markIdle("second");
assert.equal(bounded.occupiedCapacity, 2);

now = 2;
const thirdReservation = await bounded.reserve();
assert.ok(thirdReservation);
assert.equal(boundedFirst.closeCalls, 1);
assert.equal(bounded.get("first"), undefined);
assert.equal(bounded.occupiedCapacity, 2);
bounded.commit(thirdReservation, "third", boundedThird);
bounded.markIdle("third");
assert.equal(bounded.size, 2);

const busy = new McpSessionRegistry<FakeTransport>({ maxSessions: 2 });
const busyFirstReservation = await busy.reserve();
const busySecondReservation = await busy.reserve();
assert.ok(busyFirstReservation);
assert.ok(busySecondReservation);
busy.commit(busyFirstReservation, "busy-first", createTransport());
busy.commit(busySecondReservation, "busy-second", createTransport());
assert.equal(busy.occupiedCapacity, 2);
assert.equal(await busy.reserve(), undefined);
busy.markIdle("busy-first");
assert.ok(await busy.reserve());

const reservations = new McpSessionRegistry<FakeTransport>({ maxSessions: 2 });
const reservationOne = await reservations.reserve();
const reservationTwo = await reservations.reserve();
assert.ok(reservationOne);
assert.ok(reservationTwo);
assert.equal(reservations.occupiedCapacity, 2);
assert.equal(await reservations.reserve(), undefined);
reservations.release(reservationOne);
assert.ok(await reservations.reserve());

const failedEviction = new McpSessionRegistry<FakeTransport>({ maxSessions: 1 });
const failedEvictionError = new Error("eviction close failed");
const failedEvictionTransport = createTransport(failedEvictionError);
const failedEvictionInitialReservation = await failedEviction.reserve();
assert.ok(failedEvictionInitialReservation);
failedEviction.commit(
  failedEvictionInitialReservation,
  "failed-eviction",
  failedEvictionTransport,
);
failedEviction.markIdle("failed-eviction");

const failedEvictionReservation = await failedEviction.reserve();
assert.ok(failedEvictionReservation);
assert.equal(failedEvictionReservation.closeResults.length, 1);
assert.equal(failedEvictionReservation.closeResults[0]?.error, failedEvictionError);
assert.equal(failedEviction.size, 1);
assert.equal(failedEviction.occupiedCapacity, 1);
assert.throws(
  () => failedEviction.commit(failedEvictionReservation, "replacement", createTransport()),
  /reservation is not active/i,
);
assert.equal(failedEviction.get("failed-eviction"), failedEvictionTransport);

const churn = new McpSessionRegistry<FakeTransport>({ maxSessions: 64 });
for (let index = 0; index < 100; index += 1) {
  const reservation = await churn.reserve();
  assert.ok(reservation);
  churn.commit(reservation, `session-${index}`, createTransport());
  churn.markIdle(`session-${index}`);
  assert.ok(churn.occupiedCapacity <= 64);
}
assert.equal(churn.size, 64);
