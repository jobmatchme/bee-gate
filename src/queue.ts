export class ConversationQueue {
	private processing = false;
	private queue: Array<() => Promise<void>> = [];

	enqueue(work: () => Promise<void>): void {
		this.queue.push(work);
		void this.processNext();
	}

	private async processNext(): Promise<void> {
		if (this.processing || this.queue.length === 0) return;
		this.processing = true;
		const work = this.queue.shift()!;
		try {
			await work();
		} finally {
			this.processing = false;
			void this.processNext();
		}
	}
}
