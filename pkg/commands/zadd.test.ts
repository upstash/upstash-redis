import { keygen, newHttpClient, randomID } from "../test-utils";

import { afterAll, describe, expect, test } from "bun:test";
import { ZAddCommand } from "./zadd";
import { ZScoreCommand } from "./zscore";
import { ZRangeCommand } from "./zrange";
import { sleep } from "bun";

const client = newHttpClient();

const { newKey, cleanup } = keygen();
afterAll(cleanup);

describe("command format", () => {
  describe("without options", () => {
    test("build the correct command", () => {
      expect(
        new ZAddCommand(["key", { score: 0, member: "member" }]).command
      ).toEqual(["zadd", "key", 0, "member"]);
    });
  });

  describe("with nx", () => {
    test("build the correct command", () => {
      expect(
        new ZAddCommand(["key", { nx: true }, { score: 0, member: "member" }])
          .command
      ).toEqual(["zadd", "key", "nx", 0, "member"]);
    });
  });

  describe("with xx", () => {
    test("build the correct command", () => {
      expect(
        new ZAddCommand(["key", { xx: true }, { score: 0, member: "member" }])
          .command
      ).toEqual(["zadd", "key", "xx", 0, "member"]);
    });
  });

  describe("with ch", () => {
    test("build the correct command", () => {
      expect(
        new ZAddCommand(["key", { ch: true }, { score: 0, member: "member" }])
          .command
      ).toEqual(["zadd", "key", "ch", 0, "member"]);
    });
  });

  describe("with incr", () => {
    test("build the correct command", () => {
      expect(
        new ZAddCommand(["key", { incr: true }, { score: 0, member: "member" }])
          .command
      ).toEqual(["zadd", "key", "incr", 0, "member"]);
    });
  });

  describe("with nx and ch", () => {
    test("build the correct command", () => {
      expect(
        new ZAddCommand([
          "key",
          { nx: true, ch: true },
          { score: 0, member: "member" },
        ]).command
      ).toEqual(["zadd", "key", "nx", "ch", 0, "member"]);
    });
  });

  describe("with nx,ch and incr", () => {
    test("build the correct command", () => {
      expect(
        new ZAddCommand([
          "key",
          { nx: true, ch: true, incr: true },
          { score: 0, member: "member" },
        ]).command
      ).toEqual(["zadd", "key", "nx", "ch", "incr", 0, "member"]);
    });
  });

  describe("with nx and multiple members", () => {
    test("build the correct command", () => {
      expect(
        new ZAddCommand([
          "key",
          { nx: true },
          { score: 0, member: "member" },
          { score: 1, member: "member1" },
        ]).command
      ).toEqual(["zadd", "key", "nx", 0, "member", 1, "member1"]);
    });
  });
});

describe("without options", () => {
  test("adds the member", async () => {
    const key = newKey();
    const member = randomID();
    const score = Math.floor(Math.random() * 10);
    const res = await new ZAddCommand([key, { score, member }]).exec(client);
    expect(res).toEqual(1);
  });
});

describe("xx", () => {
  describe("when the element exists", () => {
    test("updates the element", async () => {
      const key = newKey();
      const member = randomID();
      const score = Math.floor(Math.random() * 10);
      await new ZAddCommand([key, { score, member }]).exec(client);
      const newScore = score + 1;
      const res = await new ZAddCommand([
        key,
        { xx: true },
        { score: newScore, member },
      ]).exec(client);
      expect(res).toEqual(0);

      const res2 = await new ZScoreCommand([key, member]).exec(client);
      expect(res2).toEqual(newScore);
    });
  });
  describe("when the element does not exist", () => {
    test("does nothing", async () => {
      const key = newKey();
      const member = randomID();
      const score = Math.floor(Math.random() * 10);
      await new ZAddCommand([key, { score, member }]).exec(client);
      const newScore = score + 1;
      const res = await new ZAddCommand([
        key,
        { xx: true },
        { score: newScore, member },
      ]).exec(client);
      expect(res).toEqual(0);
    });
  });
});

describe("nx", () => {
  describe("when the element exists", () => {
    test("does nothing", async () => {
      const key = newKey();
      const member = randomID();
      const score = Math.floor(Math.random() * 10);
      await new ZAddCommand([key, { score, member }]).exec(client);
      const newScore = score + 1;
      const res = await new ZAddCommand([
        key,
        { nx: true },
        { score: newScore, member },
      ]).exec(client);
      expect(res).toEqual(0);

      const res2 = await new ZScoreCommand([key, member]).exec(client);
      expect(res2).toEqual(score);
    });
  });
  describe("when the element does not exist", () => {
    test("creates element", async () => {
      const key = newKey();
      const member = randomID();
      const score = Math.floor(Math.random() * 10);
      const res = await new ZAddCommand([
        key,
        { nx: true },
        { score, member },
      ]).exec(client);
      expect(res).toEqual(1);
    });
  });
});

describe("ch", () => {
  test("returns the number of changed elements", async () => {
    const key = newKey();
    const member = randomID();
    const score = Math.floor(Math.random() * 10);
    await new ZAddCommand([key, { score, member }]).exec(client);
    const newScore = score + 1;
    const res = await new ZAddCommand([
      key,
      { ch: true },
      { score: newScore, member },
    ]).exec(client);
    expect(res).toEqual(1);
  });
});

describe("incr", () => {
  test("increments the score", async () => {
    const key = newKey();
    const member = randomID();
    const score = Math.floor(Math.random() * 10);
    await new ZAddCommand([key, { score, member }]).exec(client);
    const res = await new ZAddCommand([
      key,
      { incr: true },
      { score: 1, member },
    ]).exec(client);
    expect(typeof res).toBe("number");
    expect(res).toEqual(score + 1);
  });
});

describe("LT and GT", () => {
  describe("GT", () => {
    test("should replace successfully if greater than", async () => {
      const key = newKey();

      await new ZAddCommand([key, { score: 1, member: "one" }]).exec(client);
      await new ZAddCommand([key, { score: 2, member: "two" }]).exec(client);
      await new ZAddCommand([key, { score: 3, member: "three" }]).exec(client);

      await new ZAddCommand([
        key,
        { gt: true },
        { score: 4, member: "two" },
      ]).exec(client);

      const res2 = await new ZRangeCommand([
        key,
        0,
        -1,
        { withScores: true },
      ]).exec(client);

      expect(res2).toEqual(["one", 1, "three", 3, "two", 4]);
    });

    test("should fail to replace if its not greater than", async () => {
      const key = newKey();

      await new ZAddCommand([key, { score: 1, member: "one" }]).exec(client);
      await new ZAddCommand([key, { score: 2, member: "two" }]).exec(client);
      await new ZAddCommand([key, { score: 3, member: "three" }]).exec(client);

      await new ZAddCommand([
        key,
        { gt: true },
        { score: 1, member: "two" },
      ]).exec(client);

      const res2 = await new ZRangeCommand([
        key,
        0,
        -1,
        { withScores: true },
      ]).exec(client);

      expect(res2).toEqual(["one", 1, "two", 2, "three", 3]);
    });
  });

  describe("LT", () => {
    test("should replace successfully if less than", async () => {
      const key = newKey();

      await new ZAddCommand([key, { score: 1, member: "one" }]).exec(client);
      await new ZAddCommand([key, { score: 2, member: "two" }]).exec(client);
      await new ZAddCommand([key, { score: 3, member: "three" }]).exec(client);
      await new ZAddCommand([
        key,
        { lt: true },
        { score: 2, member: "three" },
      ]).exec(client);

      const res2 = await new ZRangeCommand([
        key,
        0,
        -1,
        { withScores: true },
      ]).exec(client);
      expect(res2).toEqual(["one", 1, "three", 2, "two", 2]);
    });

    test("should fail to replace if its not less than", async () => {
      const key = newKey();

      await new ZAddCommand([key, { score: 1, member: "one" }]).exec(client);
      await new ZAddCommand([key, { score: 2, member: "two" }]).exec(client);
      await new ZAddCommand([key, { score: 3, member: "three" }]).exec(client);

      await new ZAddCommand([
        key,
        { lt: true },
        { score: 6, member: "two" },
      ]).exec(client);

      const res2 = await new ZRangeCommand([
        key,
        0,
        -1,
        { withScores: true },
      ]).exec(client);

      expect(res2).toEqual(["one", 1, "two", 2, "three", 3]);
    });
  });
});
