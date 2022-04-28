import { keygen, newHttpClient } from "../test-utils.ts";
import { assertEquals } from "https://deno.land/std@0.136.0/testing/asserts.ts";
import { afterAll } from "https://deno.land/std@0.136.0/testing/bdd.ts";
import { GetCommand } from "./get.ts";
import { SetCommand } from "./set.ts";

const client = newHttpClient();

const { newKey, cleanup } = keygen();
afterAll(cleanup);
Deno.test("without options", async (t) => {
  await t.step("sets value", async () => {
    const key = newKey();
    const value = crypto.randomUUID();

    const res = await new SetCommand(key, value).exec(client);
    assertEquals(res, "OK");
    const res2 = await new GetCommand(key).exec(client);
    assertEquals(res2, value);
  });
});
Deno.test("ex", async (t) => {
  await t.step("sets value", async () => {
    const key = newKey();
    const value = crypto.randomUUID();

    const res = await new SetCommand(key, value, { ex: 1 }).exec(client);
    assertEquals(res, "OK");
    const res2 = await new GetCommand(key).exec(client);
    assertEquals(res2, value);
    await new Promise((res) => setTimeout(res, 2000));

    const res3 = await new GetCommand(key).exec(client);
    assertEquals(res3, null);
  });
});
Deno.test("px", async (t) => {
  await t.step("sets value", async () => {
    const key = newKey();
    const value = crypto.randomUUID();

    const res = await new SetCommand(key, value, { px: 1000 }).exec(client);
    assertEquals(res, "OK");
    const res2 = await new GetCommand(key).exec(client);
    assertEquals(res2, value);
    await new Promise((res) => setTimeout(res, 2000));

    const res3 = await new GetCommand(key).exec(client);

    assertEquals(res3, null);
  });
});
Deno.test("nx", async (t) => {
  await t.step("when key exists", async (t) => {
    await t.step("does nothing", async () => {
      const key = newKey();
      const value = crypto.randomUUID();
      const newValue = crypto.randomUUID();

      await new SetCommand(key, value).exec(client);
      const res = await new SetCommand(key, newValue, { nx: true }).exec(
        client,
      );
      assertEquals(res, null);
      const res2 = await new GetCommand(key).exec(client);
      assertEquals(res2, value);
    });
  });
  await t.step("when key does not exists", async (t) => {
    await t.step("overwrites key", async () => {
      const key = newKey();
      const value = crypto.randomUUID();

      const res = await new SetCommand(key, value, { nx: true }).exec(client);
      assertEquals(res, "OK");
      const res2 = await new GetCommand(key).exec(client);
      assertEquals(res2, value);
    });
  });
});
Deno.test("xx", async (t) => {
  await t.step("when key exists", async (t) => {
    await t.step("overwrites key", async () => {
      const key = newKey();
      const value = crypto.randomUUID();
      const newValue = crypto.randomUUID();

      await new SetCommand(key, value).exec(client);
      const res = await new SetCommand(key, newValue, { xx: true }).exec(
        client,
      );
      assertEquals(res, "OK");
      const res2 = await new GetCommand(key).exec(client);
      assertEquals(res2, newValue);
    });
  });
  await t.step("when key does not exists", async (t) => {
    await t.step("does nothing", async () => {
      const key = newKey();
      const value = crypto.randomUUID();

      const res = await new SetCommand(key, value, { xx: true }).exec(client);
      assertEquals(res, null);
      const res2 = await new GetCommand(key).exec(client);
      assertEquals(res2, null);
    });
  });
});
