import { mkdtempSync, readFileSync } from "fs";
import { tmpdir } from "os";
import { join } from "path";
import { describe, expect, it } from "vitest";
import { LocalFileBlobStore } from "../src/blob-store.js";

describe("LocalFileBlobStore", () => {
	it("materializes data URI artifacts into temporary files", async () => {
		const store = new LocalFileBlobStore(mkdtempSync(join(tmpdir(), "bee-blob-store-test-")));
		const materialized = await store.materialize({
			artifactId: "artifact-1",
			name: "report.csv",
			mimeType: "text/csv",
			uri: "data:text/csv;base64,bmFtZSx2YWx1ZQo=",
		});

		try {
			expect(materialized.filename).toBe("report.csv");
			expect(readFileSync(materialized.path, "utf-8")).toBe("name,value\n");
		} finally {
			await materialized.cleanup?.();
		}
	});
});
