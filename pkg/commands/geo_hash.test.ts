import { assertEquals } from "https://deno.land/std@0.203.0/assert/assert_equals.ts";
import { newHttpClient } from "../test-utils.ts";

import { GeoAddCommand } from "./geo_add.ts";
import { GeoHashCommand } from "./geo_hash.ts";

const client = newHttpClient();

Deno.test("should accept two member array and return valid hash", async () => {
  const key = "Sicily";
  const members = ["Palermo", "Catania"];
  await new GeoAddCommand([
    key,
    { longitude: 13.361389, latitude: 38.115556, member: members[0] },
    { longitude: 15.087269, latitude: 37.502669, member: members[1] },
  ]).exec(client);

  const response = await new GeoHashCommand([key, members]).exec(client);
  assertEquals(response.length, 2);
});

Deno.test("should accept three different string members and return valid hash", async () => {
  const key = "Sicily";
  const members = ["Palermo", "Catania", "Marsala"];
  await new GeoAddCommand([
    key,
    { longitude: 13.361389, latitude: 38.115556, member: members[0] },
    { longitude: 15.087269, latitude: 37.502669, member: members[1] },
    { longitude: 12.4372, latitude: 37.7981, member: members[2] },
  ]).exec(client);

  const response = await new GeoHashCommand([key, "Palermo", "Catania", "Marsala"]).exec(client);
  assertEquals(response.length, 3);
});