import { randomUUID } from "crypto";
import { copyFile, mkdir, stat, writeFile } from "fs/promises";
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
		const localPath = join(this.rootDir, ref.blobKey);
		return {
			path: localPath,
			filename:
				ref.name || ref.title || `${"artifactId" in ref ? ref.artifactId : ref.attachmentId}${extname(localPath)}`,
		};
	}
}
