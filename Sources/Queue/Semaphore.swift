/// A "semaphore" that can be awaited and signaled only once.
struct Semaphore {
    private let stream: AsyncStream<Never>
    private let continuation: AsyncStream<Never>.Continuation
    
    init() {
        (stream, continuation) = AsyncStream.makeStream()
    }
    
    /// Wait until semaphore is signaled.
    func wait() async {
        await Task {
            for await _ in stream { }
        }.value
    }
    
    /// Wait until semaphore is signaled, or the current task is cancelled.
    ///
    /// - throws: `CancellationError` if the current task is cancelled
    ///   before the semaphore is signaled.
    func waitUnlessCancelled() async throws {
        for await _ in stream { }
        try Task.checkCancellation()
    }
    
    /// Signal the semaphore.
    func signal() {
        continuation.finish()
    }
}
