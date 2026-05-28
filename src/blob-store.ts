import { randomUUID } from "crypto";
import { copyFile, mkdir, mkdtemp, rm, stat, writeFile } from "fs/promises";
import { tmpdir } from "os";
import { dirname, extname, join } from "path";
import type { ArtifactRef, AttachmentRef, BlobMaterializedFile, BlobPutInput, BlobStore } from "./types.js";

function sanitizeSegment(value: string): string {
	return value.replace(/[^a-zA-Z0-9._-]/g, "_");
}

function buildBlobKey(namespace: string, blobId: string, name?: string): string {
	const cleanNamespace = namespace
		.split("/")
		.filter(Boolean)
		.map((part) => sanitizeSegment(part))
		.join("/");
	const cleanName = name ? sanitizeSegment(name) : undefined;
	return cleanName ? `${cleanNamespace}/${blobId}-${cleanName}` : `${cleanNamespace}/${blobId}`;
}

async function ensureParent(targetPath: string): Promise<void> {
	await mkdir(dirname(targetPath), { recursive: true });
}

function extensionForMimeType(mimeType?: string): string {
	if (mimeType === "text/csv") return ".csv";
	if (mimeType === "text/html") return ".html";
	if (mimeType === "text/markdown") return ".md";
	if (mimeType === "application/json") return ".json";
	if (mimeType === "application/pdf") return ".pdf";
	if (mimeType === "image/jpeg") return ".jpg";
	if (mimeType === "image/png") return ".png";
	if (mimeType === "image/gif") return ".gif";
	if (mimeType === "image/webp") return ".webp";
	return ".bin";
}

function parseDataUri(uri: string): { mimeType?: string; data: Buffer } | undefined {
	const match = /^data:([^;,]*)(;base64)?,(.*)$/s.exec(uri);
	if (!match) return undefined;
	const mimeType = match[1] || undefined;
	const isBase64 = !!match[2];
	const payload = match[3] || "";
	return {
		mimeType,
		data: isBase64 ? Buffer.from(payload, "base64") : Buffer.from(decodeURIComponent(payload), "utf-8"),
	};
}

export class LocalFileBlobStore implements BlobStore {
	constructor(private rootDir: string) {}

	async put(input: BlobPutInput): Promise<AttachmentRef> {
		const blobId = randomUUID();
		const blobKey = buildBlobKey(input.namespace, blobId, input.name);
		const targetPath = join(this.rootDir, blobKey);
		await ensureParent(targetPath);

		if ("data" in input) {
			await writeFile(targetPath, input.data);
			return {
				attachmentId: blobId,
				blobKey,
				name: input.name,
				title: input.title || input.name,
				mimeType: input.mimeType,
				sizeBytes: input.data.byteLength,
			};
		}

		await copyFile(input.filePath, targetPath);
		const details = await stat(targetPath);
		return {
			attachmentId: blobId,
			blobKey,
			name: input.name,
			title: input.title || input.name,
			mimeType: input.mimeType,
			sizeBytes: details.size,
		};
	}

	async putArtifact(input: BlobPutInput): Promise<ArtifactRef> {
		const stored = await this.put(input);
		return {
			artifactId: stored.attachmentId,
			blobKey: stored.blobKey,
			name: stored.name,
			title: stored.title,
			mimeType: stored.mimeType,
			sizeBytes: stored.sizeBytes,
		};
	}

	async materialize(ref: AttachmentRef | ArtifactRef): Promise<BlobMaterializedFile> {
		if ("uri" in ref && ref.uri?.startsWith("data:")) {
			const parsed = parseDataUri(ref.uri);
			if (!parsed) {
				throw new Error("Invalid artifact data URI");
			}
			const tempDir = await mkdtemp(join(tmpdir(), "bee-artifact-"));
			const filename = sanitizeSegment(
				ref.name || ref.title || `${ref.artifactId}${extensionForMimeType(ref.mimeType || parsed.mimeType)}`,
			);
			const tempPath = join(tempDir, filename);
			await writeFile(tempPath, parsed.data);
			return {
				path: tempPath,
				filename,
				cleanup: async () => {
					await rm(tempDir, { recursive: true, force: true });
				},
			};
		}

		if (!ref.blobKey) {
			throw new Error("Blob reference has neither blobKey nor supported uri");
		}

		const localPath = join(this.rootDir, ref.blobKey);
		return {
			path: localPath,
			filename:
				ref.name || ref.title || `${"artifactId" in ref ? ref.artifactId : ref.attachmentId}${extname(localPath)}`,
		};
	}
}
