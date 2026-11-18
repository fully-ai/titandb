import net from "node:net";

export interface QueryResult {
  columns: string[];
  rows: unknown[][];
  rows_affected: number;
  message: string;
  explain_plan?: string;
}

/**
 * Length-prefixed TCP wire client for TitanDB.
 */
export class TitanClient {
  private socket: net.Socket;
  private authToken?: string;
  private buffer = Buffer.alloc(0);

  private constructor(socket: net.Socket, authToken?: string) {
    this.socket = socket;
    this.authToken = authToken;
  }

  static connect(host = "127.0.0.1", port = 7440, authToken?: string): Promise<TitanClient> {
    return new Promise((resolve, reject) => {
      const sock = net.createConnection({ host, port }, () => resolve(new TitanClient(sock, authToken)));
      sock.once("error", reject);
    });
  }

  query(sql: string): Promise<QueryResult> {
    return new Promise((resolve, reject) => {
      const req = JSON.stringify({ auth_token: this.authToken, op: "query", sql, params: [] });
      const payload = Buffer.from(req, "utf8");
      const frame = Buffer.alloc(4 + payload.length);
      frame.writeUInt32BE(payload.length, 0);
      payload.copy(frame, 4);

      const onData = (chunk: Buffer) => {
        this.buffer = Buffer.concat([this.buffer, chunk]);
        if (this.buffer.length < 4) return;
        const rlen = this.buffer.readUInt32BE(0);
        if (this.buffer.length < 4 + rlen) return;
        const body = this.buffer.subarray(4, 4 + rlen).toString("utf8");
        this.buffer = this.buffer.subarray(4 + rlen);
        this.socket.off("data", onData);
        const resp = JSON.parse(body);
        if (!resp.ok) reject(new Error(resp.error || "TitanDB error"));
        else resolve(resp.result);
      };
      this.socket.on("data", onData);
      this.socket.write(frame);
    });
  }

  close(): void {
    this.socket.end();
  }
}
