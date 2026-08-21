export interface ClosableMcpTransport {
  close(): Promise<void>;
}

export interface McpSessionCloseResult {
  sessionId: string;
  error?: unknown;
}

interface McpSessionEntry<TTransport> {
  transport: TTransport;
  lastActivityAt: number;
  inFlight: number;
}

export interface McpSessionRegistryOptions {
  now?: () => number;
  maxSessions?: number;
}

export interface McpSessionReservation {
  readonly token: symbol;
  readonly closeResults: readonly McpSessionCloseResult[];
}

export class McpSessionRegistry<TTransport extends ClosableMcpTransport> {
  private readonly sessions = new Map<string, McpSessionEntry<TTransport>>();
  private readonly pendingReservations = new Set<symbol>();
  private readonly now: () => number;
  private readonly maxSessions: number;

  constructor(options: McpSessionRegistryOptions = {}) {
    this.now = options.now ?? Date.now;
    this.maxSessions = options.maxSessions ?? 64;
    if (!Number.isSafeInteger(this.maxSessions) || this.maxSessions < 1) {
      throw new Error("MCP session capacity must be a positive safe integer.");
    }
  }

  get size(): number {
    return this.sessions.size;
  }

  get occupiedCapacity(): number {
    return this.sessions.size + this.pendingReservations.size;
  }

  register(sessionId: string, transport: TTransport): void {
    if (!this.sessions.has(sessionId) && this.occupiedCapacity >= this.maxSessions) {
      throw new Error("MCP session capacity is exhausted.");
    }
    this.sessions.set(sessionId, {
      transport,
      lastActivityAt: this.now(),
      inFlight: 0,
    });
  }

  get(sessionId: string): TTransport | undefined {
    const entry = this.sessions.get(sessionId);
    if (!entry) return undefined;

    entry.lastActivityAt = this.now();
    return entry.transport;
  }

  markActive(sessionId: string): TTransport | undefined {
    const entry = this.sessions.get(sessionId);
    if (!entry) return undefined;

    entry.inFlight += 1;
    entry.lastActivityAt = this.now();
    return entry.transport;
  }

  markIdle(sessionId: string): void {
    const entry = this.sessions.get(sessionId);
    if (!entry) return;

    entry.inFlight = Math.max(0, entry.inFlight - 1);
    entry.lastActivityAt = this.now();
  }

  async reserve(): Promise<McpSessionReservation | undefined> {
    let evicted:
      | { sessionId: string; entry: McpSessionEntry<TTransport> }
      | undefined;
    if (this.occupiedCapacity >= this.maxSessions) {
      let oldestIdle: [string, McpSessionEntry<TTransport>] | undefined;
      for (const candidate of this.sessions) {
        if (candidate[1].inFlight !== 0) continue;
        if (!oldestIdle || candidate[1].lastActivityAt < oldestIdle[1].lastActivityAt) {
          oldestIdle = candidate;
        }
      }
      if (!oldestIdle) return undefined;

      this.sessions.delete(oldestIdle[0]);
      evicted = { sessionId: oldestIdle[0], entry: oldestIdle[1] };
    }

    const token = Symbol("mcp-session-reservation");
    this.pendingReservations.add(token);
    const closeResults = evicted
      ? await closeSessions([
          { sessionId: evicted.sessionId, transport: evicted.entry.transport },
        ])
      : [];
    if (evicted && closeResults.some((result) => result.error !== undefined)) {
      this.pendingReservations.delete(token);
      this.sessions.set(evicted.sessionId, evicted.entry);
    }
    return { token, closeResults };
  }

  commit(
    reservation: McpSessionReservation,
    sessionId: string,
    transport: TTransport,
  ): void {
    if (!this.pendingReservations.has(reservation.token)) {
      throw new Error("MCP session reservation is not active.");
    }
    if (this.sessions.has(sessionId)) {
      this.pendingReservations.delete(reservation.token);
      throw new Error("MCP session ID is already registered.");
    }

    this.pendingReservations.delete(reservation.token);
    this.sessions.set(sessionId, {
      transport,
      lastActivityAt: this.now(),
      inFlight: 1,
    });
  }

  release(reservation: McpSessionReservation): void {
    this.pendingReservations.delete(reservation.token);
  }

  remove(sessionId: string): boolean {
    return this.sessions.delete(sessionId);
  }

  async closeIdle(idleTimeoutMs: number): Promise<McpSessionCloseResult[]> {
    const cutoff = this.now() - idleTimeoutMs;
    const idleSessions: Array<{ sessionId: string; transport: TTransport }> = [];

    for (const [sessionId, entry] of this.sessions) {
      if (entry.inFlight !== 0 || entry.lastActivityAt > cutoff) continue;

      this.sessions.delete(sessionId);
      idleSessions.push({ sessionId, transport: entry.transport });
    }

    return closeSessions(idleSessions);
  }

  async closeAll(): Promise<McpSessionCloseResult[]> {
    const sessions = Array.from(this.sessions, ([sessionId, entry]) => ({
      sessionId,
      transport: entry.transport,
    }));
    this.sessions.clear();
    this.pendingReservations.clear();
    return closeSessions(sessions);
  }
}

async function closeSessions<TTransport extends ClosableMcpTransport>(
  sessions: Array<{ sessionId: string; transport: TTransport }>,
): Promise<McpSessionCloseResult[]> {
  return Promise.all(
    sessions.map(async ({ sessionId, transport }) => {
      try {
        await transport.close();
        return { sessionId };
      } catch (error) {
        return { sessionId, error };
      }
    }),
  );
}
